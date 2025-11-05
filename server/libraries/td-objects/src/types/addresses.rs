//
// Copyright 2025 Tabs Data Inc.
//

use nonempty::{NonEmpty, nonempty};
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Deref;
use td_error::{ApiError, TdError, api_error};

/// A non-empty list of server addresses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NonEmptyAddresses(NonEmpty<SocketAddr>);

impl NonEmptyAddresses {
    pub fn new(addresses: NonEmpty<SocketAddr>) -> Self {
        Self(addresses)
    }

    pub fn from_vec(addresses: Vec<SocketAddr>) -> Result<Self, TdError> {
        let non_empty = NonEmpty::from_vec(addresses);
        if let Some(non_empty) = non_empty {
            Ok(Self(non_empty))
        } else {
            Err(api_error!(
                ApiError::InputError,
                "At least one address is required"
            ))
        }
    }
}

impl Deref for NonEmptyAddresses {
    type Target = NonEmpty<SocketAddr>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApiServerAddresses(pub NonEmptyAddresses);

impl Default for ApiServerAddresses {
    fn default() -> Self {
        const DEFAULT_PORT: u16 = 2457;
        ApiServerAddresses(NonEmptyAddresses::new(nonempty![SocketAddr::new(
            Ipv4Addr::LOCALHOST.into(),
            DEFAULT_PORT
        )]))
    }
}

impl From<ApiServerAddresses> for NonEmptyAddresses {
    fn from(v: ApiServerAddresses) -> Self {
        v.0
    }
}

impl Deref for ApiServerAddresses {
    type Target = NonEmptyAddresses;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InternalServerAddresses(pub NonEmptyAddresses);

impl Deref for InternalServerAddresses {
    type Target = NonEmptyAddresses;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<InternalServerAddresses> for NonEmptyAddresses {
    fn from(v: InternalServerAddresses) -> Self {
        v.0
    }
}

impl Default for InternalServerAddresses {
    fn default() -> Self {
        const DEFAULT_PORT: u16 = 2458;
        InternalServerAddresses(NonEmptyAddresses::new(nonempty![SocketAddr::new(
            Ipv4Addr::LOCALHOST.into(),
            DEFAULT_PORT
        )]))
    }
}
