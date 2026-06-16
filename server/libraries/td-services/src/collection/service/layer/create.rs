//
// Copyright 2025 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::types::basic::{CollectionType, GroupId};
use td_tower::extractors::Ref;

/// Infer the collection flavour from its resolved group id. The mapping lives on
/// [`CollectionType::from_group_id`] so the create service and the test seed
/// helpers agree on the rule (the seeded system groups carry the same well-known
/// id in every project).
pub async fn collection_type_from_group(group_id: Ref<GroupId>) -> Result<CollectionType, TdError> {
    Ok(CollectionType::from_group_id(&group_id))
}
