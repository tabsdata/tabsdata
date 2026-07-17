//
// Copyright 2026 Tabsdata Inc.
//

//! Project-scope extractors reading the [`basic::WildcardProject`] that
//! [`project_resolver`](crate::layers::project_resolver) places on the request.
//! An endpoint declares whether it tolerates the `*` wildcard by which one it
//! uses — [`Project`] rejects it, [`WildcardProject`] accepts it.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::TdError;
use td_objects::types::basic;
use td_objects::types::basic::ProjectId;
use td_services::project::ProjectError;

/// A single concrete project. Rejects the `*` wildcard.
pub struct Project(pub ProjectId);

/// The resolved scope: a concrete project or the `*` wildcard.
pub struct WildcardProject(pub basic::WildcardProject);

impl<S: Send + Sync> FromRequestParts<S> for Project {
    type Rejection = ErrorStatus;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<basic::WildcardProject>() {
            Some(basic::WildcardProject::Project(id)) => Ok(Project(*id)),
            Some(basic::WildcardProject::Wildcard) => {
                Err(TdError::from(ProjectError::WildcardProjectNotAllowed).into())
            }
            None => Err(TdError::from(ProjectError::MissingProjectSegment).into()),
        }
    }
}

impl<S: Send + Sync> FromRequestParts<S> for WildcardProject {
    type Rejection = ErrorStatus;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<basic::WildcardProject>()
            .copied()
            .map(WildcardProject)
            .ok_or_else(|| TdError::from(ProjectError::MissingProjectSegment).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    fn parts_with(scope: Option<basic::WildcardProject>) -> Parts {
        let mut parts = Request::builder().body(()).unwrap().into_parts().0;
        if let Some(scope) = scope {
            parts.extensions.insert(scope);
        }
        parts
    }

    #[tokio::test]
    async fn project_accepts_concrete() {
        let pid = ProjectId::default();
        let mut parts = parts_with(Some(basic::WildcardProject::Project(pid)));
        let Ok(Project(id)) = Project::from_request_parts(&mut parts, &()).await else {
            panic!("expected a concrete project");
        };
        assert_eq!(id, pid);
    }

    #[tokio::test]
    async fn project_rejects_wildcard() {
        let mut parts = parts_with(Some(basic::WildcardProject::Wildcard));
        assert!(Project::from_request_parts(&mut parts, &()).await.is_err());
    }

    #[tokio::test]
    async fn project_rejects_missing_segment() {
        let mut parts = parts_with(None);
        assert!(Project::from_request_parts(&mut parts, &()).await.is_err());
    }

    #[tokio::test]
    async fn wildcard_accepts_concrete() {
        let pid = ProjectId::default();
        let mut parts = parts_with(Some(basic::WildcardProject::Project(pid)));
        let Ok(WildcardProject(scope)) = WildcardProject::from_request_parts(&mut parts, &()).await
        else {
            panic!("expected a concrete project");
        };
        assert_eq!(scope, basic::WildcardProject::Project(pid));
    }

    #[tokio::test]
    async fn wildcard_accepts_wildcard() {
        let mut parts = parts_with(Some(basic::WildcardProject::Wildcard));
        let Ok(WildcardProject(scope)) = WildcardProject::from_request_parts(&mut parts, &()).await
        else {
            panic!("expected the wildcard");
        };
        assert_eq!(scope, basic::WildcardProject::Wildcard);
    }

    #[tokio::test]
    async fn wildcard_rejects_missing_segment() {
        let mut parts = parts_with(None);
        assert!(
            WildcardProject::from_request_parts(&mut parts, &())
                .await
                .is_err()
        );
    }
}
