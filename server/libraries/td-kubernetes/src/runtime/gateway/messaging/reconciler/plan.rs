//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use td_bridge::observer::{ObservedWorkload, WorkloadKind, WorkloadPhase};

pub(crate) const WARN_AFTER: Duration = Duration::from_secs(30);
pub(crate) const FORCE_AFTER: Duration = Duration::from_secs(5 * 60);

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
    forced: bool,
}

impl Deletion {
    fn new(kind: WorkloadKind, worker_id: Option<String>) -> Self {
        Self {
            kind,
            worker_id,
            issued: None,
            warned: false,
            forced: false,
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
}

impl<P> Default for State<P> {
    fn default() -> Self {
        Self {
            creates: HashMap::new(),
            deletions: BTreeMap::new(),
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
            return;
        }
        let kind = key.kind;
        self.deletions
            .entry(name.to_owned())
            .or_insert_with(|| Deletion::new(kind, Some(worker_id.to_owned())));
    }

    pub(crate) fn restore(&mut self, key: Key, submission: Submission<P>) {
        if self.deletions.contains_key(&submission.name) {
            return;
        }
        self.creates.entry(key).or_insert(submission);
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
        self.deletions
            .retain(|name, _| present.contains_key(name.as_str()));

        self.creates
            .retain(|_, submission| !present.contains_key(submission.name.as_str()));

        let mut ready = Vec::new();
        let mut healing = Vec::new();
        for (key, submission) in &self.creates {
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
            let Some(issued) = deletion.issued else {
                deletion.issued = Some(now);
                actions.push(Action::Delete {
                    kind: deletion.kind,
                    name: name.clone(),
                });
                continue;
            };
            let elapsed = now.saturating_duration_since(issued);
            if elapsed >= FORCE_AFTER && !deletion.forced {
                deletion.forced = true;
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
                actions.push(Action::Create { key, submission });
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests;
