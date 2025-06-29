//
//  Copyright 2024 Tabs Data Inc.
//

mod test_server;

#[cfg(test)]
mod tests {
    use crate::test_server::{TEST_GET, TEST_POST};
    use td_apiforge::apiserver_docs;
    use utoipa::openapi::path::Operation;
    use utoipa::{Modify, OpenApi};

    struct SecurityAddon;
    impl Modify for SecurityAddon {
        fn modify(&self, _openapi: &mut utoipa::openapi::OpenApi) {}
    }

    #[apiserver_docs(
        title = "tests",
        version = "1",
        modifier = &SecurityAddon,
        tags_attribute = apiserver_tag,
        paths_attribute = apiserver_path,
        schemas_attribute = apiserver_schema,
        crate_dir(name = "crate", dir = "facets/td-apiforge/tests")
    )]
    pub struct Docs;

    #[test]
    fn test_title_and_version() {
        let openapi = Docs::openapi();
        assert_eq!(openapi.info.title, "tests");
        assert_eq!(openapi.info.version, "1");
    }

    #[test]
    fn test_paths() {
        let openapi = Docs::openapi();
        assert_eq!(openapi.paths.paths.len(), 2);

        if let Some(path_item) = &openapi.paths.paths.get(TEST_GET) {
            assert!(path_item.get.is_some());
            let operation = path_item.get.clone().unwrap();
            assert_test_path(&operation);
        } else {
            panic!("Path {TEST_GET} not found");
        }

        if let Some(path_item) = &openapi.paths.paths.get(TEST_POST) {
            assert!(path_item.post.is_some());
            let operation = path_item.post.clone().unwrap();
            assert_test_path(&operation);
        } else {
            panic!("Path {TEST_GET} not found");
        }
    }

    fn assert_test_path(operation: &Operation) {
        assert_eq!(operation.tags, Some(vec![String::from("Test")]));
        assert_eq!(operation.security.clone().unwrap().len(), 1);
        assert_eq!(operation.parameters.clone().unwrap().len(), 2);

        let params = operation.parameters.clone().unwrap();
        assert_eq!(params.len(), 2);
        assert!(params.iter().any(|param| param.name == "tid"));
        assert!(params.iter().any(|param| param.name == "page"));

        let request = operation.request_body.clone().unwrap().content;
        assert_eq!(request.len(), 1);
        assert!(request.contains_key("application/json"));

        let responses = operation.responses.clone().responses;
        assert_eq!(responses.len(), 2);
        assert!(responses.contains_key("200"));
        assert!(responses.contains_key("500"));
    }

    #[test]
    fn test_components() {
        let openapi = Docs::openapi();
        let components = openapi.components.as_ref().expect("Components not found");

        let schemas = &components.schemas;
        assert!(schemas.contains_key("TestRequest"));
        assert!(schemas.contains_key("TestResponse"));
        assert!(schemas.contains_key("TestErrorResponse"));
        assert!(schemas.contains_key("CtxGetStatusConcreteResponse"));
    }
}
