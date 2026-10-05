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
//! - A key is `(function_id, kind)`, kind being function run, cron trigger
//!   or stage trigger. A trigger and the runs of the same function have
//!   different keys and never affect each other.
//!
//! # State
//!
//! - `creates`: at most one pending create per key (the latest Start).
//! - `deletions`: pending deletions, by workload name.
//! - `settling`: names whose create was issued but that have not been seen
//!   listed yet. A create that timed out may still land in the background.
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
//!
//! # One pass (`plan`)
//!
//! 1. Settling names that are now listed, or older than `CREATE_SETTLE`,
//!    stop settling.
//! 2. Every listed workload Kubernetes is already deleting is tracked as a
//!    deletion (adoption), whoever started it.
//! 3. A deletion whose workload is not listed is done and forgotten, unless
//!    its name is settling: the workload may still appear and must then go.
//! 4. A pending create whose workload is listed, not being deleted and not
//!    pending deletion is done and forgotten. If it is being deleted or
//!    pending deletion (Stop W, then Start W), the create is held until the
//!    old workload is gone.
//! 5. A pending create is held while its name is settling or still listed.
//!    Otherwise every other live workload of its key (any phase but
//!    Succeeded/Failed) is queued for deletion (self-heal: one workload per
//!    key), and the create waits until none is left. Finished workloads are
//!    left alone so their logs survive until their TTL.
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

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use td_bridge::observer::{ObservedWorkload, WorkloadKind, WorkloadPhase};

pub(crate) const WARN_AFTER: Duration = Duration::from_secs(30);
pub(crate) const FORCE_AFTER: Duration = Duration::from_secs(5 * 60);
pub(crate) const FORCE_RETRY: Duration = Duration::from_secs(60);
pub(crate) const CREATE_SETTLE: Duration = Duration::from_secs(60);
pub(crate) const MAX_CREATE_ATTEMPTS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    pub function_id: String,
    pub kind: WorkloadKind,
}

impl Key {
    fn of(workload: &ObservedWorkload) -> Option<Self> {
        workload.function_id.as_ref().map(|function_id| Self {
            function_id: function_id.to_string(),
            kind: workload.kind,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Submission<P> {
    pub worker_id: String,
    pub name: String,
    pub payload: P,
    pub attempts: u32,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action<P> {
    Delete { kind: WorkloadKind, name: String },
    Force { name: String, worker_id: String },
    Warn { name: String, elapsed: Duration },
    Create { key: Key, submission: Submission<P> },
}

#[derive(Debug)]
struct Deletion {
    kind: WorkloadKind,
    worker_id: Option<String>,
    issued: Option<Instant>,
    warned: bool,
    forced: Option<Instant>,
}

impl Deletion {
    fn new(kind: WorkloadKind, worker_id: Option<String>) -> Self {
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
pub(crate) struct State<P> {
    creates: HashMap<Key, Submission<P>>,
    deletions: BTreeMap<String, Deletion>,
    settling: HashMap<String, Instant>,
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
        self.creates.insert(key, submission)
    }

    pub(crate) fn withdraw(&mut self, key: Key, worker_id: &str, name: &str) {
        if self
            .creates
            .get(&key)
            .is_some_and(|submission| submission.worker_id == worker_id)
        {
            self.creates.remove(&key);
            if !self.settling.contains_key(name) {
                return;
            }
        }
        let kind = key.kind;
        self.deletions
            .entry(name.to_owned())
            .or_insert_with(|| Deletion::new(kind, Some(worker_id.to_owned())));
    }

    pub(crate) fn restore(
        &mut self,
        key: Key,
        mut submission: Submission<P>,
    ) -> Option<Submission<P>> {
        if self.deletions.contains_key(&submission.name) {
            return None;
        }
        submission.attempts += 1;
        if submission.attempts >= MAX_CREATE_ATTEMPTS {
            return Some(submission);
        }
        self.creates.entry(key).or_insert(submission);
        None
    }

    pub(crate) fn reissue(&mut self, name: &str) {
        if let Some(deletion) = self.deletions.get_mut(name) {
            deletion.issued = None;
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.creates.is_empty() && self.deletions.is_empty()
    }

    pub(crate) fn plan(&mut self, observed: &[ObservedWorkload], now: Instant) -> Vec<Action<P>> {
        let present: HashMap<&str, &ObservedWorkload> = observed
            .iter()
            .map(|workload| (workload.name.as_str(), workload))
            .collect();

        self.settling.retain(|name, since| {
            !present.contains_key(name.as_str())
                && now.saturating_duration_since(*since) < CREATE_SETTLE
        });

        for workload in observed.iter().filter(|workload| workload.deleting) {
            self.deletions
                .entry(workload.name.clone())
                .or_insert_with(|| Deletion {
                    issued: Some(now),
                    ..Deletion::new(
                        workload.kind,
                        workload.worker_id.as_ref().map(ToString::to_string),
                    )
                });
        }
        let settling = &self.settling;
        self.deletions
            .retain(|name, _| present.contains_key(name.as_str()) || settling.contains_key(name));

        let deletions = &self.deletions;
        self.creates.retain(
            |_, submission| match present.get(submission.name.as_str()) {
                Some(workload) => workload.deleting || deletions.contains_key(&submission.name),
                None => true,
            },
        );

        let mut ready = Vec::new();
        let mut healing = Vec::new();
        for (key, submission) in &self.creates {
            if self.settling.contains_key(&submission.name)
                || present.contains_key(submission.name.as_str())
            {
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
            }
            healing.extend(blockers.into_iter().filter(|workload| !workload.deleting));
        }
        for workload in healing {
            self.deletions
                .entry(workload.name.clone())
                .or_insert_with(|| {
                    Deletion::new(
                        workload.kind,
                        workload.worker_id.as_ref().map(ToString::to_string),
                    )
                });
        }

        let mut actions = Vec::new();
        for (name, deletion) in &mut self.deletions {
            if !present.contains_key(name.as_str()) {
                continue;
            }
            let Some(issued) = deletion.issued else {
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
                if let Some(worker_id) = &deletion.worker_id {
                    actions.push(Action::Force {
                        name: name.clone(),
                        worker_id: worker_id.clone(),
                    });
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
                self.settling.insert(submission.name.clone(), now);
                actions.push(Action::Create { key, submission });
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests;
