//
// Copyright 2026 Tabsdata Inc.
//

//! Decision logic of the workload reconciler: given what was requested and
//! one observation of the cluster, decide which Kubernetes actions to take.
//! It never calls Kubernetes itself; `run` executes the returned actions.
//!
//! # Identity
//!
//! - A workload is identified by its name, derived from its worker id.
//! - A function run's key is its worker: every attempt has its own worker, so
//!   two attempts of one function (a run and a replay) never affect each
//!   other. Running both is safe: a superseded attempt's reports are answered
//!   stale and its data never becomes current.
//! - A trigger's key is `(function_id, kind)`, kind being cron trigger or
//!   stage trigger, so a newer version of a trigger displaces the older one.
//!
//! # State
//!
//! - `creates`: at most one pending create per key (the latest Start).
//! - `deletions`: pending deletions, by workload name.
//! - `settling`: names whose create was planned but that have not been seen
//!   listed yet. A create that timed out may still land in the background.
//!   The window starts when the create call returns (`issued`), not when it
//!   is planned: a pass's actions run in turn, so a create can wait long for
//!   its call.
//!
//! # Inputs
//!
//! - `submit` (Start W): W becomes the key's pending create, replacing an
//!   older one that was never created.
//! - `withdraw` (Stop W): only W is affected. If W is the key's pending
//!   create it is cancelled; otherwise, or if an earlier create of W may
//!   still land (settling), W's workload is queued for deletion. A Stop never
//!   touches other workers of the key.
//! - `restore` (a create failed): the create is retried, unless a Stop for W
//!   arrived meanwhile, a newer Start replaced it, or it already failed
//!   `MAX_CREATE_ATTEMPTS` times (then it is given up and recovery sees the
//!   run's workload missing).
//! - `reissue` (a delete call failed): the delete is issued again next pass.
//! - `issued` (a create call returned): its name's settle window starts.
//!
//! # One pass (`plan`)
//!
//! 1. Settling names that are now listed, or issued more than
//!    `CREATE_SETTLE` ago, stop settling.
//! 2. Every listed workload Kubernetes is already deleting is tracked as a
//!    deletion (adoption), whoever started it.
//! 3. A deletion whose workload is not listed is done and forgotten, unless
//!    its name is settling: the workload may still appear and must then go.
//! 4. A pending create whose workload is listed, not being deleted and not
//!    pending deletion is done and forgotten. If it is being deleted or
//!    pending deletion (Stop W, then Start W), the create is held until the
//!    old workload is gone.
//! 5. A pending create is held while its name is settling or still listed.
//!    Otherwise the create waits while any other live workload of its key
//!    (any phase but Succeeded/Failed) is listed, and each such workload is
//!    queued for deletion at once (self-heal: one workload per key). Only a
//!    trigger's key can hold another workload. Finished workloads are left
//!    alone so their logs survive until their TTL.
//! 6. Each listed deletion is issued once; after `WARN_AFTER` it warns once;
//!    after `FORCE_AFTER` it is forced, and forced again every
//!    `FORCE_RETRY` while the workload is still listed.
//! 7. Ready creates are issued and their names start settling, which also
//!    spaces retries of a failing create by `CREATE_SETTLE`.
//!
//! # Guarantees
//!
//! - While the process runs, no accepted Start or Stop is lost: a Start ends
//!   with its workload created, replaced by a newer Start, cancelled by a
//!   Stop, or given up after `MAX_CREATE_ATTEMPTS`; a Stop ends with its
//!   workload gone.
//! - At most one live workload per key, except while the previous one is
//!   being deleted.
//! - The newest request of a key wins; a Stop for an older worker never
//!   removes or drops a newer one.
//! - The state lives in memory. After a restart, recovery restores
//!   consistency: a lost create shows as a missing workload, a lost
//!   deletion as a workload no attempt expects.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use td_bridge::observer::{ObservedWorkload, WorkloadKind, WorkloadPhase};
use td_objects::types::basic::WorkerId;
use tracing::info;

pub(crate) const WARN_AFTER: Duration = Duration::from_secs(30);
pub(crate) const FORCE_AFTER: Duration = Duration::from_secs(5 * 60);
pub(crate) const FORCE_RETRY: Duration = Duration::from_secs(60);
pub(crate) const CREATE_SETTLE: Duration = Duration::from_secs(60);
pub(crate) const MAX_CREATE_ATTEMPTS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Key {
    Run(WorkerId),
    Trigger {
        function_id: String,
        kind: WorkloadKind,
    },
}

impl Key {
    fn of(workload: &ObservedWorkload) -> Option<Self> {
        match workload.kind {
            WorkloadKind::Function => workload.worker_id.clone().map(Self::Run),
            kind => workload
                .function_id
                .as_ref()
                .map(|function_id| Self::Trigger {
                    function_id: function_id.to_string(),
                    kind,
                }),
        }
    }

    fn kind(&self) -> WorkloadKind {
        match self {
            Self::Run(_) => WorkloadKind::Function,
            Self::Trigger { kind, .. } => *kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Submission<P> {
    pub worker_id: WorkerId,
    pub name: String,
    pub payload: P,
    pub attempts: u32,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action<P> {
    Delete { kind: WorkloadKind, name: String },
    Force { name: String, worker_id: WorkerId },
    Warn { name: String, elapsed: Duration },
    Create { key: Key, submission: Submission<P> },
}

#[derive(Debug)]
struct Deletion {
    kind: WorkloadKind,
    worker_id: Option<WorkerId>,
    issued: Option<Instant>,
    warned: bool,
    forced: Option<Instant>,
}

impl Deletion {
    fn new(kind: WorkloadKind, worker_id: Option<WorkerId>) -> Self {
        Self {
            kind,
            worker_id,
            issued: None,
            warned: false,
            forced: None,
        }
    }
}

fn live(workload: &ObservedWorkload) -> bool {
    !matches!(
        workload.phase,
        WorkloadPhase::Succeeded | WorkloadPhase::Failed
    )
}

#[derive(Debug)]
struct Settling {
    key: Key,
    issued: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct State<P> {
    creates: HashMap<Key, Submission<P>>,
    deletions: BTreeMap<String, Deletion>,
    settling: HashMap<String, Settling>,
}

impl<P> Default for State<P> {
    fn default() -> Self {
        Self {
            creates: HashMap::new(),
            deletions: BTreeMap::new(),
            settling: HashMap::new(),
        }
    }
}

impl<P> State<P> {
    pub(crate) fn submit(&mut self, key: Key, submission: Submission<P>) -> Option<Submission<P>> {
        info!(
            workload = %submission.name,
            key = ?key,
            "reconciler: start requested"
        );
        let replaced = self.creates.insert(key, submission);
        if let Some(replaced) = &replaced {
            info!(
                workload = %replaced.name,
                "reconciler: queued create replaced by a newer start"
            );
        }
        replaced
    }

    pub(crate) fn withdraw(&mut self, key: Key, worker_id: &WorkerId, name: &str) {
        if self
            .creates
            .get(&key)
            .is_some_and(|submission| &submission.worker_id == worker_id)
        {
            self.creates.remove(&key);
            if !self.settling.contains_key(name) {
                info!(workload = %name, "reconciler: stop cancelled its queued create");
                return;
            }
            info!(
                workload = %name,
                "reconciler: stop cancelled its queued create; an earlier create may still land"
            );
        }
        let kind = key.kind();
        if !self.deletions.contains_key(name) {
            info!(workload = %name, "reconciler: stop queued a deletion");
        }
        self.deletions
            .entry(name.to_owned())
            .or_insert_with(|| Deletion::new(kind, Some(worker_id.clone())));
    }

    pub(crate) fn restore(
        &mut self,
        key: Key,
        mut submission: Submission<P>,
    ) -> Option<Submission<P>> {
        if self.deletions.contains_key(&submission.name) {
            info!(
                workload = %submission.name,
                "reconciler: failed create not retried; a stop arrived meanwhile"
            );
            return None;
        }
        submission.attempts += 1;
        if submission.attempts >= MAX_CREATE_ATTEMPTS {
            return Some(submission);
        }
        if self.creates.contains_key(&key) {
            info!(
                workload = %submission.name,
                "reconciler: failed create not retried; a newer start replaced it"
            );
            return None;
        }
        info!(
            workload = %submission.name,
            attempts = submission.attempts,
            "reconciler: failed create queued for retry"
        );
        self.creates.insert(key, submission);
        None
    }

    pub(crate) fn reissue(&mut self, name: &str) {
        if let Some(deletion) = self.deletions.get_mut(name) {
            info!(workload = %name, "reconciler: failed delete queued for retry");
            deletion.issued = None;
        }
    }

    pub(crate) fn issued(&mut self, name: &str, now: Instant) {
        if let Some(settling) = self.settling.get_mut(name) {
            settling.issued = Some(now);
        }
    }

    /// The function runs whose Start was accepted and whose workload is not
    /// listed yet.
    pub(crate) fn queued_runs(&self) -> HashSet<WorkerId> {
        self.creates
            .keys()
            .chain(self.settling.values().map(|settling| &settling.key))
            .filter_map(|key| match key {
                Key::Run(worker_id) => Some(worker_id.clone()),
                Key::Trigger { .. } => None,
            })
            .collect()
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.creates.is_empty() && self.deletions.is_empty()
    }

    pub(crate) fn plan(&mut self, observed: &[ObservedWorkload], now: Instant) -> Vec<Action<P>> {
        let present: HashMap<&str, &ObservedWorkload> = observed
            .iter()
            .map(|workload| (workload.name.as_str(), workload))
            .collect();

        self.settling.retain(|name, settling| {
            if present.contains_key(name.as_str()) {
                info!(workload = %name, "reconciler: created workload is now listed");
                return false;
            }
            if settling
                .issued
                .is_some_and(|issued| now.saturating_duration_since(issued) >= CREATE_SETTLE)
            {
                info!(workload = %name, "reconciler: created workload never listed; stops settling");
                return false;
            }
            true
        });

        for workload in observed.iter().filter(|workload| workload.deleting) {
            if self.deletions.contains_key(&workload.name) {
                continue;
            }
            info!(workload = %workload.name, "reconciler: adopted a deletion already in progress");
            self.deletions.insert(
                workload.name.clone(),
                Deletion {
                    issued: Some(now),
                    ..Deletion::new(workload.kind, workload.worker_id.clone())
                },
            );
        }
        let settling = &self.settling;
        self.deletions.retain(|name, _| {
            if present.contains_key(name.as_str()) || settling.contains_key(name) {
                return true;
            }
            info!(workload = %name, "reconciler: deletion done; workload is gone");
            false
        });

        let deletions = &self.deletions;
        self.creates.retain(
            |_, submission| match present.get(submission.name.as_str()) {
                Some(workload)
                    if !workload.deleting && !deletions.contains_key(&submission.name) =>
                {
                    info!(workload = %submission.name, "reconciler: create done; workload exists");
                    false
                }
                _ => true,
            },
        );

        let mut ready = Vec::new();
        let mut healing = Vec::new();
        for (key, submission) in &self.creates {
            if self.settling.contains_key(&submission.name) {
                info!(workload = %submission.name, "reconciler: create held; a previous create may still land");
                continue;
            }
            if present.contains_key(submission.name.as_str()) {
                info!(workload = %submission.name, "reconciler: create held; its old workload is still listed");
                continue;
            }
            let blockers: Vec<&ObservedWorkload> = observed
                .iter()
                .filter(|workload| {
                    workload.name != submission.name
                        && live(workload)
                        && Key::of(workload).as_ref() == Some(key)
                })
                .collect();
            if blockers.is_empty() {
                ready.push(key.clone());
            } else {
                info!(
                    workload = %submission.name,
                    blockers = ?blockers.iter().map(|workload| &workload.name).collect::<Vec<_>>(),
                    "reconciler: create held; other live workloads of the function"
                );
            }
            healing.extend(blockers.into_iter().filter(|workload| !workload.deleting));
        }
        for workload in healing {
            if !self.deletions.contains_key(&workload.name) {
                info!(
                    workload = %workload.name,
                    "reconciler: blocking workload queued for deletion"
                );
            }
            self.deletions
                .entry(workload.name.clone())
                .or_insert_with(|| Deletion::new(workload.kind, workload.worker_id.clone()));
        }

        let mut actions = Vec::new();
        for (name, deletion) in &mut self.deletions {
            if !present.contains_key(name.as_str()) {
                continue;
            }
            let Some(issued) = deletion.issued else {
                info!(workload = %name, "reconciler: issuing delete");
                deletion.issued = Some(now);
                actions.push(Action::Delete {
                    kind: deletion.kind,
                    name: name.clone(),
                });
                continue;
            };
            let elapsed = now.saturating_duration_since(issued);
            let force_due = deletion
                .forced
                .is_none_or(|forced| now.saturating_duration_since(forced) >= FORCE_RETRY);
            if elapsed >= FORCE_AFTER && force_due {
                deletion.forced = Some(now);
                match &deletion.worker_id {
                    Some(worker_id) => {
                        info!(workload = %name, "reconciler: issuing force delete");
                        actions.push(Action::Force {
                            name: name.clone(),
                            worker_id: worker_id.clone(),
                        });
                    }
                    None => info!(workload = %name, "reconciler: cannot force; worker id unknown"),
                }
            } else if elapsed >= WARN_AFTER && !deletion.warned {
                deletion.warned = true;
                actions.push(Action::Warn {
                    name: name.clone(),
                    elapsed,
                });
            }
        }
        for key in ready {
            if let Some(submission) = self.creates.remove(&key) {
                info!(workload = %submission.name, attempts = submission.attempts, "reconciler: issuing create");
                self.settling.insert(
                    submission.name.clone(),
                    Settling {
                        key: key.clone(),
                        issued: None,
                    },
                );
                actions.push(Action::Create { key, submission });
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests;
