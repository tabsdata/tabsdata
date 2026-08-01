//
// Copyright 2025 Tabsdata Inc.
//

use http::uri::Authority;
use nonempty::{NonEmpty, nonempty};
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::str::FromStr;
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
            Ipv4Addr::UNSPECIFIED.into(),
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
            Ipv4Addr::UNSPECIFIED.into(),
            DEFAULT_PORT
        )]))
    }
}

/// A non-empty list of authorities to dial. Unlike [`NonEmptyAddresses`], which
/// answers "where do I listen" and so must be numeric, these name a peer and are
/// resolved by whoever dials them: a service name here survives the address
/// behind it changing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonEmptyAuthorities(NonEmpty<Authority>);

impl NonEmptyAuthorities {
    pub fn new(authorities: NonEmpty<Authority>) -> Self {
        Self(authorities)
    }

    pub fn from_vec(authorities: Vec<Authority>) -> Result<Self, TdError> {
        NonEmpty::from_vec(authorities)
            .map(Self)
            .ok_or_else(|| api_error!(ApiError::InputError, "At least one authority is required"))
    }
}

impl Deref for NonEmptyAuthorities {
    type Target = NonEmpty<Authority>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for NonEmptyAuthorities {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let rendered: Vec<&str> = self.0.iter().map(Authority::as_str).collect();
        rendered.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NonEmptyAuthorities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw: Vec<String> = Vec::deserialize(deserializer)?;
        let parsed = raw
            .iter()
            .map(|entry| Authority::from_str(entry.trim()).map_err(serde::de::Error::custom))
            .collect::<Result<Vec<_>, _>>()?;
        NonEmpty::from_vec(parsed)
            .map(Self)
            .ok_or_else(|| serde::de::Error::custom("At least one authority is required"))
    }
}

/// Where workers reach the internal server: the bound addresses, plus the
/// authority to put in the callback urls handed to them.
///
/// A bind address answers "where do I listen", which a wildcard answers well
/// and a caller in another network namespace cannot use. Carrying the authority
/// as a name, resolved by whoever dials it, keeps a callback valid across
/// restarts that change the address behind that name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalServerEndpoint {
    addresses: InternalServerAddresses,
    authority: Authority,
}

impl InternalServerEndpoint {
    /// The port always comes from the bound address, so an advertised authority
    /// carrying its own is overridden rather than silently disagreeing with the
    /// listener.
    pub fn new(
        addresses: InternalServerAddresses,
        advertised: Option<&Authority>,
    ) -> Result<Self, TdError> {
        let bound = addresses.first();
        let authority = match advertised {
            None => bound.to_string(),
            Some(advertised) => format!("{}:{}", advertised.host(), bound.port()),
        };
        let authority = Authority::from_str(&authority).map_err(|error| {
            api_error!(
                ApiError::InputError,
                "'{}' is not a valid authority: {}",
                authority,
                error
            )
        })?;
        Ok(Self {
            addresses,
            authority,
        })
    }

    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    pub fn addresses(&self) -> &InternalServerAddresses {
        &self.addresses
    }
}

impl Default for InternalServerEndpoint {
    fn default() -> Self {
        let addresses = InternalServerAddresses::default();
        Self::new(addresses, None).expect("a bound address is always a valid authority")
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn unset_advertised_keeps_the_bound_address() {
        let endpoint =
            InternalServerEndpoint::new(InternalServerAddresses::default(), None).unwrap();
        assert_eq!(endpoint.authority().as_str(), "0.0.0.0:2458");
    }

    #[test]
    fn advertised_host_keeps_the_bound_port() {
        let advertised = Authority::from_str("td-apiserver.ns.svc.cluster.local:9999").unwrap();
        let endpoint =
            InternalServerEndpoint::new(InternalServerAddresses::default(), Some(&advertised))
                .unwrap();
        assert_eq!(
            endpoint.authority().as_str(),
            "td-apiserver.ns.svc.cluster.local:2458"
        );
    }

    #[test]
    fn advertised_host_without_a_port_takes_the_bound_one() {
        let advertised = Authority::from_str("td-apiserver.ns.svc.cluster.local").unwrap();
        let endpoint =
            InternalServerEndpoint::new(InternalServerAddresses::default(), Some(&advertised))
                .unwrap();
        assert_eq!(
            endpoint.authority().as_str(),
            "td-apiserver.ns.svc.cluster.local:2458"
        );
    }
}
