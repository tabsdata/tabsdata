//
// Copyright 2025 Tabs Data Inc.
//

#![allow(private_bounds, non_camel_case_types)]

use td_authz::Authz;
use td_authz::tower::InterColl;
use td_error::TdError;
use td_objects::dxo::inter_collection_access::{
    InterCollectionAccess, InterCollectionAccessBuilder,
};
use td_objects::tower_service::with::With;
use td_tower::from_fn::from_fn;
use td_tower::{layer, layers};

pub trait DoAuthz {
    fn do_authz() -> bool;
}

pub struct DO_AUTHZ;
impl DoAuthz for DO_AUTHZ {
    fn do_authz() -> bool {
        true
    }
}

pub struct SKIP_AUTHZ;
impl DoAuthz for SKIP_AUTHZ {
    fn do_authz() -> bool {
        false
    }
}

#[layer]
pub fn inter_collection_authz<A, T, E>()
where
    A: DoAuthz,
    for<'a> T: Send + Sync + 'a,
    InterCollectionAccessBuilder: for<'a> TryFrom<&'a T, Error = E>,
    for<'a> E: Into<TdError> + 'a,
{
    if A::do_authz() {
        layers!(
            // inter collection authz check
            from_fn(With::<T>::vec_convert_to::<InterCollectionAccessBuilder, _>),
            from_fn(With::<Vec<InterCollectionAccessBuilder>>::build::<Vec<InterCollectionAccess>>),
            from_fn(Authz::<InterColl>::check_inter_collection),
        )
    } else {
        layers!()
    }
}
