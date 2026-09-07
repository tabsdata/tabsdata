//
// Copyright 2026 Tabsdata Inc.
//

//! What an upgrade is about to do, decided before it runs so it can be logged
//! up front and reported once it is done.

/// The version transition [`UpgradeTransaction::upgrade_db_version`](super::UpgradeTransaction::upgrade_db_version)
/// takes on one schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeAction {
    Install {
        version: usize,
    },
    Ladder {
        from: usize,
        to: usize,
        targets: Vec<usize>,
    },
    UpToDate {
        version: usize,
    },
    /// Written by a newer binary than this one — refused, never upgraded down.
    Newer {
        database: usize,
        binary: usize,
    },
}

/// What one component's schema did in an upgrade run. `action` is `None` for a
/// component whose `upgrade` never reached the version ladder.
#[derive(Debug, Clone)]
pub struct ComponentUpgrade {
    pub key: &'static str,
    pub schema: String,
    pub action: Option<UpgradeAction>,
}

/// The action taking a schema recorded at `stored` to `current`.
pub fn upgrade_action(stored: Option<usize>, current: usize) -> UpgradeAction {
    match stored {
        None => UpgradeAction::Install { version: current },
        Some(version) if version == current => UpgradeAction::UpToDate { version },
        Some(version) if version < current => UpgradeAction::Ladder {
            from: version,
            to: current,
            targets: ((version + 1)..=current).collect(),
        },
        Some(version) => UpgradeAction::Newer {
            database: version,
            binary: current,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{UpgradeAction, upgrade_action};

    #[test]
    fn no_stored_version_installs() {
        assert_eq!(
            upgrade_action(None, 7),
            UpgradeAction::Install { version: 7 }
        );
    }

    #[test]
    fn same_version_is_up_to_date() {
        assert_eq!(
            upgrade_action(Some(7), 7),
            UpgradeAction::UpToDate { version: 7 }
        );
    }

    #[test]
    fn older_version_walks_every_intermediate_delta() {
        assert_eq!(
            upgrade_action(Some(4), 7),
            UpgradeAction::Ladder {
                from: 4,
                to: 7,
                targets: vec![5, 6, 7],
            }
        );
    }

    #[test]
    fn newer_version_is_refused() {
        assert_eq!(
            upgrade_action(Some(9), 7),
            UpgradeAction::Newer {
                database: 9,
                binary: 7,
            }
        );
    }
}
