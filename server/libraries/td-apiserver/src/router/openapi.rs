//
// Copyright 2024 Tabs Data Inc.
//

//! OpenApi service for the API Server. Routes are discovered and documented automatically, from
//! the crates folders and files configured.

use axum::Router;
use td_apiforge::apiserver_docs;
use td_build::version::TABSDATA_VERSION;
use td_objects::rest_urls::{DOCS_URL, OPENAPI_JSON_URL};
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::{SwaggerUi, Url};

pub fn router() -> Router {
    struct SecurityAddon;
    impl Modify for SecurityAddon {
        fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
            let components: &mut utoipa::openapi::Components = openapi.components.as_mut().unwrap();
            components.add_security_scheme(
                "Token",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }

    // Sadly, url must be a literal for now. This is a limitation of the utoipa macro.
    #[apiserver_docs(
        title = "Tabsdata API",
        version = TABSDATA_VERSION,
        modifier = &SecurityAddon,
        server = (url = "/api/v1", description = "API V1 Server"),
    )]
    struct ApiServerDocs;

    SwaggerUi::new(DOCS_URL)
        .url(
            Url::new("API V1 Docs", OPENAPI_JSON_URL),
            ApiServerDocs::openapi(),
        )
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_docs_router() {
        let app = router();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{}/", DOCS_URL))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{}/", DOCS_URL))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_openapi_json() {
        let app = router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(OPENAPI_JSON_URL)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!body.is_empty());

        // Assert relevant parts of the JSON
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // Assert info
        assert_eq!(json["info"]["title"], "Tabsdata API");

        // Assert paths
        assert!(json["paths"].as_object().unwrap().contains_key("/users"));
        assert!(json["paths"]
            .as_object()
            .unwrap()
            .contains_key("/collections"));
        assert!(json["paths"]
            .as_object()
            .unwrap()
            .contains_key("/collections/{collection}/functions"));

        // Assert tags
        let tags = json["tags"].as_array().unwrap();
        assert!(tags.iter().any(|tag| tag["name"] == "Auth"));
        assert!(tags.iter().any(|tag| tag["name"] == "Internal"));
        assert!(tags.iter().any(|tag| tag["name"] == "Collections"));
        assert!(tags.iter().any(|tag| tag["name"] == "Execution"));
        assert!(tags.iter().any(|tag| tag["name"] == "Functions"));
        assert!(tags.iter().any(|tag| tag["name"] == "Authz"));
        assert!(tags.iter().any(|tag| tag["name"] == "Status"));
        assert!(tags.iter().any(|tag| tag["name"] == "Users"));
    }

    #[ignore]
    #[tokio::test]
    async fn test_no_orphan_schemas() {
        use std::collections::HashSet;

        let app = router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(OPENAPI_JSON_URL)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!body.is_empty());

        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let schemas = json["components"]["schemas"].as_object().unwrap();
        let mut referenced_schemas = HashSet::new();

        // Function to recursively find all "$ref" values
        fn find_refs(value: &serde_json::Value, refs: &mut HashSet<String>) {
            match value {
                serde_json::Value::Object(map) => {
                    for (key, val) in map {
                        if key == "$ref" {
                            if let Some(ref_str) = val.as_str() {
                                refs.insert(
                                    ref_str
                                        .trim_start_matches("#/components/schemas/")
                                        .to_string(),
                                );
                            }
                        } else {
                            find_refs(val, refs);
                        }
                    }
                }
                serde_json::Value::Array(arr) => {
                    for val in arr {
                        find_refs(val, refs);
                    }
                }
                _ => {}
            }
        }

        // Start the search from the root of the JSON
        find_refs(&json, &mut referenced_schemas);

        // Check that all referenced schemas exist
        for schema_name in &referenced_schemas {
            assert!(
                schemas.contains_key(schema_name),
                "Schema {} is referenced but not defined",
                schema_name
            );
        }

        // Check that all defined schemas are referenced
        for schema_name in schemas.keys() {
            assert!(
                referenced_schemas.contains(schema_name),
                "Schema {} is defined but not used",
                schema_name
            );
        }
    }

    #[tokio::test]
    async fn test_no_orphan_tags() {
        use std::collections::HashSet;

        let app = router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(OPENAPI_JSON_URL)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!body.is_empty());

        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let defined_tags: HashSet<_> = json["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag["name"].as_str().unwrap().to_string())
            .collect();

        let mut used_tags = HashSet::new();

        // Function to recursively find all tags in paths
        fn find_tags(value: &serde_json::Value, tags: &mut HashSet<String>) {
            match value {
                serde_json::Value::Object(map) => {
                    for (key, val) in map {
                        if key == "tags" {
                            if let serde_json::Value::Array(tag_array) = val {
                                for tag in tag_array {
                                    if let Some(tag_str) = tag.as_str() {
                                        tags.insert(tag_str.to_string());
                                    }
                                }
                            }
                        } else {
                            find_tags(val, tags);
                        }
                    }
                }
                serde_json::Value::Array(arr) => {
                    for val in arr {
                        find_tags(val, tags);
                    }
                }
                _ => {}
            }
        }

        // Start the search from the paths section of the JSON
        find_tags(&json["paths"], &mut used_tags);

        // Check that all used tags are defined
        for tag in &used_tags {
            assert!(
                defined_tags.contains(tag),
                "Tag {} is used but not defined",
                tag
            );
        }

        // Check that all defined tags are used
        for tag in &defined_tags {
            assert!(
                used_tags.contains(tag),
                "Tag {} is defined but not used",
                tag
            );
        }
    }

    #[tokio::test]
    async fn test_security_schema() {
        let app = router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(OPENAPI_JSON_URL)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!body.is_empty());

        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let security_schemes = json["components"]["securitySchemes"].as_object().unwrap();
        assert!(security_schemes.contains_key("Token"));
        assert_eq!(security_schemes["Token"]["type"], "http");
        assert_eq!(security_schemes["Token"]["scheme"], "bearer");
    }

    #[tokio::test]
    async fn test_paths_and_params_consistency() {
        let app = router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(OPENAPI_JSON_URL)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!body.is_empty());

        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let paths = json["paths"].as_object().unwrap();

        for (path, path_item) in paths {
            // Extract parameter names from the URL path
            let url_param_names: Vec<&str> = path
                .split('/')
                .filter_map(|segment| {
                    if segment.starts_with('{') && segment.ends_with('}') {
                        Some(&segment[1..segment.len() - 1])
                    } else {
                        None
                    }
                })
                .collect();

            // Check that the parameters defined in the path match the parameters in the URL
            if let Some(methods) = path_item.as_object() {
                for (method, details) in methods {
                    if let Some(params) = details["parameters"].as_array() {
                        let url_params_defined: Vec<&str> = params
                            .iter()
                            .filter(|param| param["in"] == "path")
                            .filter_map(|param| param["name"].as_str())
                            .collect();
                        assert_eq!(
                            url_param_names, url_params_defined,
                            "URL parameter names do not match parameters for path {} and method {}",
                            path, method
                        );
                    }
                }
            }
        }
    }
}
