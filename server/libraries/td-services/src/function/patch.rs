//
// Copyright 2026 Tabsdata Inc.
//

use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{CollectionId, FunctionIdName, FunctionStatus, FunctionVersionId};
use td_queries::CurrentEntry;

#[td_error]
pub enum CreateFunctionPatchError {
    #[error("patch_of {0} resolves to a function in a different collection than the URL")]
    WrongCollection(FunctionVersionId) = 0,
    #[error("patch_of {0} resolves to a different function than the one named in the URL")]
    WrongFunction(FunctionVersionId) = 1,
    #[error("patch_of {0} resolves to a deleted function")]
    DeletedParent(FunctionVersionId) = 2,
}

/// Validate that the parent fvid (`patch_of`) resolves to a function in the
/// URL's collection, with the URL's function name/id, and not deleted.
pub async fn assert_patch_parent_matches_url(
    conn: &mut DomainConnection<Apiserver>,
    parent: &FunctionDB,
    url_collection: &CollectionId,
    url_function: &FunctionIdName,
) -> Result<(), TdError> {
    let parent = parent;
    if parent.collection_id != *url_collection {
        Err(CreateFunctionPatchError::WrongCollection(parent.id.clone()))?
    }
    let url_function = url_function;
    let url_matches = match url_function {
        FunctionIdName::Id(id) => *id == parent.function_id,
        FunctionIdName::Name(name) => *name == parent.name,
    };
    if !url_matches {
        Err(CreateFunctionPatchError::WrongFunction(parent.id.clone()))?
    }
    // Deletion tombstones a fresh version row — earlier rows stay Active —
    // so aliveness is the *current* row's status, not the parent row's.
    let current: FunctionDB = conn
        .current()
        .fetch_one::<FunctionDB>(&parent.function_id)
        .await?;
    if matches!(current.status, FunctionStatus::Deleted) {
        Err(CreateFunctionPatchError::DeletedParent(parent.id.clone()))?
    }
    Ok(())
}
