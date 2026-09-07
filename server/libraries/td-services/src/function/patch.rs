//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::function::FunctionDB;
use td_objects::types::basic::{CollectionId, FunctionIdName, FunctionVersionId};
use td_queries::Queries;

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
pub async fn assert_patch_parent_matches_url<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    parent: &FunctionDB,
    url_collection: &CollectionId,
    url_function: &FunctionIdName,
) -> Result<(), TdError> {
    if parent.base.collection_id != *url_collection {
        Err(CreateFunctionPatchError::WrongCollection(
            parent.base.id.clone(),
        ))?
    }
    let url_matches = match url_function {
        FunctionIdName::Id(id) => *id == parent.base.function_id,
        FunctionIdName::Name(name) => *name == parent.base.name,
    };
    if !url_matches {
        Err(CreateFunctionPatchError::WrongFunction(
            parent.base.id.clone(),
        ))?
    }
    // Deletion closes the function's open interval, so aliveness is whether a
    // current row exists, not the parent row's status.
    let current = conn
        .latest()
        .fetch_optional::<FunctionDB>(&parent.base.function_id)
        .await?;
    if current.is_none() {
        Err(CreateFunctionPatchError::DeletedParent(
            parent.base.id.clone(),
        ))?
    }
    Ok(())
}
