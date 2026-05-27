//
// Copyright 2026 Tabs Data Inc.
//

use crate::group::GroupError;
use td_error::TdError;
use td_objects::dxo::group::GroupDBWithNames;
use td_tower::extractors::Ref;

/// Rejects mutation of a row whose `system` flag is true. The seeded
/// `default` / `source` / `destination` groups are protected from
/// update and delete by this guard.
pub async fn assert_not_system(group: Ref<GroupDBWithNames>) -> Result<(), TdError> {
    if *group.system {
        Err(GroupError::SystemGroup(group.name.clone()))?
    }
    Ok(())
}
