//
// Copyright 2026 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::Buildable;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::function::{FunctionDB, FunctionPatch, FunctionPatchDB};
use td_objects::types::basic::{CollectionId, FunctionIdName, FunctionStatus, FunctionVersionId};
use td_tower::extractors::Ref;

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
    parent: Ref<FunctionDB>,
    url_collection: Ref<CollectionId>,
    url_function: Ref<FunctionIdName>,
) -> Result<(), TdError> {
    let parent = parent.deref();
    if parent.collection_id != *url_collection {
        Err(CreateFunctionPatchError::WrongCollection(parent.id.clone()))?
    }
    let url_function = url_function.deref();
    let url_matches = match url_function {
        FunctionIdName::Id(id) => *id == parent.function_id,
        FunctionIdName::Name(name) => *name == parent.name,
    };
    if !url_matches {
        Err(CreateFunctionPatchError::WrongFunction(parent.id.clone()))?
    }
    if matches!(parent.status, FunctionStatus::Deleted) {
        Err(CreateFunctionPatchError::DeletedParent(parent.id.clone()))?
    }
    Ok(())
}

/// Build the new `FunctionPatchDB` row from the parent function +
/// request body. Patches live in their own table; fields not set in
/// the body fall back to parent values.
pub async fn build_patch_function(
    parent: Ref<FunctionDB>,
    body: Ref<FunctionPatch>,
    request: Ref<RequestContext>,
) -> Result<FunctionPatchDB, TdError> {
    let parent = parent.deref();
    let body = body.deref();
    let request = request.deref();

    Ok(FunctionPatchDB::builder()
        .id(FunctionVersionId::new())
        .parent_function_version_id(parent.id.clone())
        .function_id(parent.function_id)
        .collection_id(parent.collection_id)
        .snippet(body.snippet.clone())
        .bundle_id(body.bundle_id)
        .runtime_values(
            body.runtime_values
                .clone()
                .unwrap_or_else(|| parent.runtime_values.clone()),
        )
        .description(
            body.description
                .clone()
                .unwrap_or_else(|| parent.description.clone()),
        )
        .defined_on(request.time)
        .defined_by_id(request.user_id)
        .status(FunctionStatus::Active)
        .build()?)
}
