//
// Copyright 2026 Tabsdata Inc.
//

//! Which runs are held on hold. A run is held while a run producing one of its
//! inputs is Failed or OnHold or, for a subscriber, while a transaction it
//! waits for is stuck: one with a Failed or OnHold run, or reading from an
//! uncommitted transaction that is stuck. A plan's transactions never read
//! each other in a cycle, so the walk never comes back to the run's own.

use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use td_database::access::{AccessMode, SeesProjectRows};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::types::basic::{LogicalFunctionRunId, TransactionId};
use td_queries::Queries;

/// Answers which runs are held, walking upstream only, one statement per
/// level, and only as far as the runs still unanswered need. It remembers
/// what it fetched for one unit of work, so the writer that owns it reports
/// the runs it puts on hold or releases.
#[derive(Default)]
pub(crate) struct HoldEvaluator {
    producers: HashMap<LogicalFunctionRunId, Vec<LogicalFunctionRunId>>,
    awaited: HashMap<LogicalFunctionRunId, Vec<TransactionId>>,
    reads: HashMap<TransactionId, Vec<TransactionId>>,
    stuck: HashMap<TransactionId, Vec<LogicalFunctionRunId>>,
    /// Transactions with no Failed or OnHold run anywhere upstream.
    clean: HashSet<TransactionId>,
}

struct Walk<'a> {
    run: &'a LogicalFunctionRunId,
    seen: HashSet<TransactionId>,
    frontier: Vec<TransactionId>,
}

impl HoldEvaluator {
    /// The held runs among `runs`, each with the Failed or OnHold runs found
    /// holding it. The runs of `recovered` hold nothing.
    pub(crate) async fn held<S: SeesProjectRows, M: AccessMode>(
        &mut self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        runs: &[LogicalFunctionRunId],
        recovered: &HashSet<LogicalFunctionRunId>,
    ) -> Result<HashMap<LogicalFunctionRunId, Vec<LogicalFunctionRunId>>, TdError> {
        self.fetch_producers(conn, runs).await?;
        let mut held = HashMap::new();
        let mut rest = Vec::new();
        for run in runs {
            let blocking = unrecovered(&self.producers[run], recovered);
            if blocking.is_empty() {
                rest.push(run);
            } else {
                held.insert(run.clone(), blocking);
            }
        }

        self.fetch_awaited(conn, &rest).await?;
        let mut walks: Vec<Walk> = rest
            .into_iter()
            .filter_map(|run| {
                let seen: HashSet<TransactionId> = self.awaited[run]
                    .iter()
                    .filter(|transaction| !self.clean.contains(*transaction))
                    .cloned()
                    .collect();
                (!seen.is_empty()).then(|| Walk {
                    run,
                    frontier: seen.iter().cloned().collect(),
                    seen,
                })
            })
            .collect();
        while !walks.is_empty() {
            let transactions = unfetched(&walks, &self.stuck);
            self.fetch_stuck(conn, &transactions).await?;
            let mut open = Vec::new();
            for walk in walks {
                let blocking: Vec<LogicalFunctionRunId> = walk
                    .frontier
                    .iter()
                    .flat_map(|transaction| unrecovered(&self.stuck[transaction], recovered))
                    .collect();
                if blocking.is_empty() {
                    open.push(walk);
                } else {
                    held.insert(walk.run.clone(), blocking);
                }
            }

            let transactions = unfetched(&open, &self.reads);
            self.fetch_reads(conn, &transactions).await?;
            walks = Vec::new();
            for mut walk in open {
                let next: Vec<TransactionId> = walk
                    .frontier
                    .iter()
                    .flat_map(|transaction| &self.reads[transaction])
                    .filter(|read| !self.clean.contains(*read))
                    .filter(|read| walk.seen.insert((*read).clone()))
                    .cloned()
                    .collect();
                if !next.is_empty() {
                    walk.frontier = next;
                    walks.push(walk);
                } else if walk.seen.iter().all(|seen| self.stuck[seen].is_empty()) {
                    self.clean.extend(walk.seen);
                }
            }
        }
        Ok(held)
    }

    /// Forgets what a run put on hold since may have changed.
    pub(crate) fn mark_held(&mut self) {
        self.producers.clear();
        self.stuck.clear();
        self.clean.clear();
    }

    pub(crate) fn mark_released(&mut self, runs: &[LogicalFunctionRunId]) {
        let released: HashSet<&LogicalFunctionRunId> = runs.iter().collect();
        for producers in self.producers.values_mut() {
            producers.retain(|producer| !released.contains(producer));
        }
        for stuck in self.stuck.values_mut() {
            stuck.retain(|run| !released.contains(run));
        }
    }

    async fn fetch_producers<S: SeesProjectRows, M: AccessMode>(
        &mut self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        runs: &[LogicalFunctionRunId],
    ) -> Result<(), TdError> {
        let missing: Vec<LogicalFunctionRunId> = runs
            .iter()
            .filter(|run| !self.producers.contains_key(*run))
            .unique()
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let found = conn.logical_runs().blocking_producers(&missing).await?;
        record(Step::Producers, found.len());
        for run in missing {
            self.producers.entry(run).or_default();
        }
        for (run, producer) in found {
            self.producers.entry(run).or_default().push(producer);
        }
        Ok(())
    }

    async fn fetch_awaited<S: SeesProjectRows, M: AccessMode>(
        &mut self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        runs: &[&LogicalFunctionRunId],
    ) -> Result<(), TdError> {
        let missing: Vec<LogicalFunctionRunId> = runs
            .iter()
            .copied()
            .filter(|run| !self.awaited.contains_key(*run))
            .unique()
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let found = conn.logical_runs().awaited_transactions(&missing).await?;
        record(Step::Awaited, found.len());
        for run in missing {
            self.awaited.entry(run).or_default();
        }
        for (run, transaction) in found {
            self.awaited.entry(run).or_default().push(transaction);
        }
        Ok(())
    }

    async fn fetch_stuck<S: SeesProjectRows, M: AccessMode>(
        &mut self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        transactions: &[TransactionId],
    ) -> Result<(), TdError> {
        if transactions.is_empty() {
            return Ok(());
        }
        let found = conn.logical_runs().stuck_runs(transactions).await?;
        record(Step::Stuck, found.len());
        for transaction in transactions {
            self.stuck.entry(transaction.clone()).or_default();
        }
        for (transaction, run) in found {
            self.stuck.entry(transaction).or_default().push(run);
        }
        Ok(())
    }

    async fn fetch_reads<S: SeesProjectRows, M: AccessMode>(
        &mut self,
        conn: &mut DomainConnection<Apiserver, S, M>,
        transactions: &[TransactionId],
    ) -> Result<(), TdError> {
        if transactions.is_empty() {
            return Ok(());
        }
        let found = conn
            .logical_runs()
            .transactions_read_by(transactions)
            .await?;
        record(Step::Reads, found.len());
        for transaction in transactions {
            self.reads.entry(transaction.clone()).or_default();
        }
        for (reader, read) in found {
            self.reads.entry(reader).or_default().push(read);
        }
        Ok(())
    }
}

fn unrecovered(
    runs: &[LogicalFunctionRunId],
    recovered: &HashSet<LogicalFunctionRunId>,
) -> Vec<LogicalFunctionRunId> {
    runs.iter()
        .filter(|run| !recovered.contains(*run))
        .cloned()
        .collect()
}

fn unfetched<V>(walks: &[Walk], known: &HashMap<TransactionId, V>) -> Vec<TransactionId> {
    walks
        .iter()
        .flat_map(|walk| &walk.frontier)
        .filter(|transaction| !known.contains_key(*transaction))
        .unique()
        .cloned()
        .collect()
}

enum Step {
    Producers,
    Awaited,
    Stuck,
    Reads,
}

#[cfg(not(test))]
fn record(_: Step, _: usize) {}

#[cfg(test)]
fn record(step: Step, rows: usize) {
    let _ = tests::STATEMENTS.try_with(|statements| statements.borrow_mut().record(step, rows));
}

#[cfg(test)]
pub(crate) mod tests {
    use super::Step;
    use std::cell::RefCell;

    /// The statements a [`HoldEvaluator`](super::HoldEvaluator) issued inside
    /// a [`STATEMENTS`] scope, and the rows they returned.
    #[derive(Clone, Copy, Debug, Default)]
    pub(crate) struct Statements {
        pub(crate) producers: usize,
        pub(crate) awaited: usize,
        pub(crate) stuck: usize,
        pub(crate) reads: usize,
        pub(crate) rows: usize,
    }

    impl Statements {
        pub(super) fn record(&mut self, step: Step, rows: usize) {
            let count = match step {
                Step::Producers => &mut self.producers,
                Step::Awaited => &mut self.awaited,
                Step::Stuck => &mut self.stuck,
                Step::Reads => &mut self.reads,
            };
            *count += 1;
            self.rows += rows;
        }

        pub(crate) fn total(&self) -> usize {
            self.producers + self.awaited + self.stuck + self.reads
        }
    }

    tokio::task_local! {
        pub(crate) static STATEMENTS: RefCell<Statements>;
    }
}
