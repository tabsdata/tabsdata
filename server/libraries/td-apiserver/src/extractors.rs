//
// Copyright 2026 Tabsdata Inc.
//

//! Project-scope extractor reading the [`ProjectId`] that
//! [`project_resolver`](crate::layers::project_resolver) places on the request.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use ta_apiserver::status::error_status::ErrorStatus;
use td_error::TdError;
use td_objects::types::basic::ProjectId;
use td_services::project::ProjectError;

/// The concrete project resolved from the `{project}` URL segment.
pub struct Project(pub ProjectId);

impl<S: Send + Sync> FromRequestParts<S> for Project {
    type Rejection = ErrorStatus;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<ProjectId>()
            .copied()
            .map(Project)
            .ok_or_else(|| TdError::from(ProjectError::MissingProjectSegment).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    fn parts_with(project: Option<ProjectId>) -> Parts {
        let mut parts = Request::builder().body(()).unwrap().into_parts().0;
        if let Some(project) = project {
            parts.extensions.insert(project);
        }
        parts
    }

    #[tokio::test]
    async fn project_accepts_concrete() {
        let pid = ProjectId::default();
        let mut parts = parts_with(Some(pid));
        let Ok(Project(id)) = Project::from_request_parts(&mut parts, &()).await else {
            panic!("expected a concrete project");
        };
        assert_eq!(id, pid);
    }

    #[tokio::test]
    async fn project_rejects_missing_segment() {
        let mut parts = parts_with(None);
        assert!(Project::from_request_parts(&mut parts, &()).await.is_err());
    }
}
