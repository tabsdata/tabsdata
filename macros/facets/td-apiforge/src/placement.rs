//
//  Copyright 2026 Tabsdata Inc.
//

//! Where a route sits, as flags: `version(v1)` (required), `scope(project)`,
//! `security_mode(rbac|simple)` and `internal`. Each maps to a field of the registry's
//! `RouterRegistration`; an absent flag is the plain value.

use darling::util::Flag;
use proc_macro2::TokenStream;
use quote::quote;

use crate::auth::{Auth, AuthSet};
use crate::word::word_enum;

word_enum!(Version, "version", { "v1" => V1 });
word_enum!(Scope, "scope", { "project" => Project });
word_enum!(SecurityMode, "security_mode", { "rbac" => Rbac, "simple" => Simple });
word_enum!(Method, "method", {
    "get" => Get,
    "post" => Post,
    "put" => Put,
    "patch" => Patch,
    "delete" => Delete,
    "any" => Any,
});

impl Method {
    /// The `#[utoipa::path]` verb. It only shapes the generated operation:
    /// the registry documents the route under the verbs `method` serves.
    pub fn utoipa_verb(&self) -> TokenStream {
        match self {
            Method::Get | Method::Any => quote!(get),
            Method::Post => quote!(post),
            Method::Put => quote!(put),
            Method::Patch => quote!(patch),
            Method::Delete => quote!(delete),
        }
    }

    pub fn axum_routing(&self) -> TokenStream {
        match self {
            Method::Get => quote!(axum::routing::get),
            Method::Post => quote!(axum::routing::post),
            Method::Put => quote!(axum::routing::put),
            Method::Patch => quote!(axum::routing::patch),
            Method::Delete => quote!(axum::routing::delete),
            Method::Any => quote!(axum::routing::any),
        }
    }
}

/// `/projects/{project}` itself is a flat route keyed by the segment; the
/// resolver runs only below it.
const PROJECT_PREFIX: &str = "/projects/{project}/";
const INTERNAL_PREFIX: &str = "/internal/";

pub struct Placement {
    pub version: Version,
    pub scope: Option<Scope>,
    pub security_mode: Option<SecurityMode>,
    pub internal: Flag,
}

impl Placement {
    /// The combinations the topology composes; `RouterRegistration::validate`
    /// re-checks hand-written registrations with the same rules.
    /// Placement is also spelled by the path and the handler's extractors;
    /// the spellings must agree.
    pub fn validate(
        &self,
        auth: &AuthSet,
        path: &str,
        project_extractor: bool,
    ) -> darling::Result<()> {
        let open = auth.0 == [Auth::None];
        let internal = auth.0 == [Auth::Internal];
        let service_token = auth.0 == [Auth::ServiceToken];
        let project_path = path.starts_with(PROJECT_PREFIX);
        let internal_path = path.starts_with(INTERNAL_PREFIX);
        if self.internal.is_present() != internal_path {
            return Err(darling::Error::custom(format!(
                "`internal` routes live under `{INTERNAL_PREFIX}` and nothing else does"
            )));
        }
        if (self.scope == Some(Scope::Project)) != project_path {
            return Err(darling::Error::custom(format!(
                "`scope(project)` routes live under `{PROJECT_PREFIX}` and nothing else does"
            )));
        }
        if (self.scope == Some(Scope::Project)) != project_extractor {
            return Err(darling::Error::custom(
                "`scope(project)` routes take the `Project` extractor and nothing else does",
            ));
        }
        if self.internal.is_present() {
            if self.scope.is_some() || self.security_mode.is_some() {
                return Err(darling::Error::custom(
                    "`internal` routes take neither `scope` nor `security_mode`",
                )
                .with_span(&self.internal.span()));
            }
            if !internal && !service_token {
                return Err(darling::Error::custom(
                    "`internal` routes accept exactly `auth(internal)` or `auth(service_token)`",
                )
                .with_span(&self.internal.span()));
            }
            return Ok(());
        }
        if internal {
            return Err(darling::Error::custom(
                "`auth(internal)` is only valid on `internal` routes",
            ));
        }
        if self.scope.is_some() && self.security_mode.is_some() {
            return Err(darling::Error::custom(
                "`scope(project)` routes are not mode-specific",
            ));
        }
        if self.scope.is_some() && open {
            return Err(darling::Error::custom(
                "`scope(project)` routes need an identity for the resolver; `auth(none)` is not valid",
            ));
        }
        Ok(())
    }

    pub fn server(&self) -> TokenStream {
        if self.internal.is_present() {
            quote!(::ta_apiserver::router::Server::Internal)
        } else {
            quote!(::ta_apiserver::router::Server::Public)
        }
    }

    pub fn version(&self) -> TokenStream {
        let version = self.version;
        quote!(Some(#version))
    }

    pub fn scope(&self) -> TokenStream {
        match self.scope {
            Some(scope) => quote!(#scope),
            None => quote!(::ta_apiserver::router::Scope::Flat),
        }
    }

    pub fn modes(&self) -> TokenStream {
        match self.security_mode {
            Some(mode) => quote!(&[#mode]),
            None => quote!(::ta_apiserver::router::SecurityMode::ALL),
        }
    }
}
