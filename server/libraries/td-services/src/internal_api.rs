//
// Copyright 2025 Tabsdata Inc.
//

//! Tokenized requests against the internal API. The route templates live on
//! the `td-apiserver` handlers that serve them and are injected at boot, so
//! token scopes always match the served templates.

use http::Method;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use ta_services::service::Ctx;
use td_common::jwt::{JwtConfig, encode_internal_token};
use td_common::worker::{Callback, HttpCallback, HttpRequest};
use td_error::td_error;
use td_objects::dxo::function::FunctionVersionParam;
use td_objects::dxo::physical_function_run::FunctionRunParam;
use td_objects::dxo::worker::WorkerParam;
use td_objects::types::addresses::InternalServerEndpoint;
use td_objects::types::basic::{FunctionVersionId, PhysicalFunctionRunId, WorkerId};
use td_objects::{PathParams, Route};
use url::Url;

/// The internal API's route templates, relative to `base` (where the apiserver
/// mounts its API).
#[derive(Debug, Clone, Copy, Default)]
pub struct InternalRoutes {
    pub base: &'static str,
    pub worker_callback: Route<WorkerParam>,
    pub worker_message: Route<WorkerParam>,
    pub function_run_callback: Route<(FunctionRunParam, WorkerParam)>,
    pub function_version_execute: Route<FunctionVersionParam>,
    pub function_version_execution_status: Route<FunctionVersionParam>,
    pub download_function_state: Route<FunctionVersionParam>,
}

#[td_error]
pub enum WorkerRequestError {
    #[error("Error parsing URL: {0}")]
    UrlParse(#[from] url::ParseError) = 5000,
    #[error("Error minting internal token: {0}")]
    Token(#[from] td_common::jwt::JwtError) = 5001,
}

/// The internal API bound to the running internal server.
#[derive(Clone)]
pub struct InternalApi {
    routes: InternalRoutes,
    jwt: Ctx<JwtConfig>,
    endpoint: Arc<InternalServerEndpoint>,
}

impl InternalApi {
    pub fn new(
        routes: InternalRoutes,
        jwt: Ctx<JwtConfig>,
        endpoint: Arc<InternalServerEndpoint>,
    ) -> Self {
        Self {
            routes,
            jwt,
            endpoint,
        }
    }

    /// Fills the route with the params to form the concrete URL and mints a
    /// token bound to `(method, scope = base+template, bindings)`; the
    /// internal router validates that token against the actual request.
    fn request<P: PathParams>(
        &self,
        method: Method,
        route: Route<P>,
        params: &P,
        mut headers: HashMap<String, String>,
        body: bool,
    ) -> Result<HttpRequest, WorkerRequestError> {
        let base = self.routes.base;
        let url = Url::parse(&format!(
            "http://{}{base}{}",
            self.endpoint.authority(),
            route.fill(params)
        ))
        .map_err(WorkerRequestError::UrlParse)?;
        let scope = format!("{base}{}", route.template());
        let bindings: BTreeMap<String, String> = P::names()
            .into_iter()
            .map(|name| name.to_string())
            .zip(params.values())
            .collect();
        let token = encode_internal_token(&self.jwt, method.as_str(), &scope, bindings)?;
        headers.insert("authorization".to_string(), format!("Bearer {token}"));
        Ok(HttpRequest {
            url,
            method,
            headers,
            body,
        })
    }

    fn post_callback<P: PathParams>(
        &self,
        route: Route<P>,
        params: &P,
    ) -> Result<Callback, WorkerRequestError> {
        let request: HttpCallback =
            self.request(Method::POST, route, params, HashMap::new(), true)?;
        Ok(Callback::Http(request))
    }

    fn get_callback<P: PathParams>(
        &self,
        route: Route<P>,
        params: &P,
    ) -> Result<Callback, WorkerRequestError> {
        let request: HttpCallback =
            self.request(Method::GET, route, params, HashMap::new(), false)?;
        Ok(Callback::Http(request))
    }

    /// POST: where a worker reports its own lifecycle status.
    pub fn worker_responder(&self, worker_id: &WorkerId) -> Result<Callback, WorkerRequestError> {
        self.post_callback(
            self.routes.worker_callback,
            &WorkerParam {
                worker: worker_id.clone(),
            },
        )
    }

    /// GET: where a worker pulls its own message (kubernetes pull path).
    pub fn worker_resolver(&self, worker_id: &WorkerId) -> Result<Callback, WorkerRequestError> {
        self.get_callback(
            self.routes.worker_message,
            &WorkerParam {
                worker: worker_id.clone(),
            },
        )
    }

    /// POST: where a function worker reports the status of the run it executes.
    pub fn function_run_responder(
        &self,
        function_run_id: &PhysicalFunctionRunId,
        worker_id: &WorkerId,
    ) -> Result<Callback, WorkerRequestError> {
        self.post_callback(
            self.routes.function_run_callback,
            &(
                FunctionRunParam {
                    function_run: function_run_id.clone(),
                },
                WorkerParam {
                    worker: worker_id.clone(),
                },
            ),
        )
    }

    /// POST: the trigger a stage/cron worker calls to execute its function version.
    pub fn function_version_execute(
        &self,
        function_version_id: &FunctionVersionId,
        headers: HashMap<String, String>,
    ) -> Result<HttpRequest, WorkerRequestError> {
        self.request(
            Method::POST,
            self.routes.function_version_execute,
            &FunctionVersionParam {
                function_version: function_version_id.clone(),
            },
            headers,
            true,
        )
    }

    /// GET: the function's persisted stage state.
    pub fn download_function_state(
        &self,
        function_version_id: &FunctionVersionId,
    ) -> Result<HttpRequest, WorkerRequestError> {
        self.request(
            Method::GET,
            self.routes.download_function_state,
            &FunctionVersionParam {
                function_version: function_version_id.clone(),
            },
            HashMap::new(),
            false,
        )
    }

    /// GET: the execution status of the function version a stage worker triggered.
    pub fn function_version_execution_status(
        &self,
        function_version_id: &FunctionVersionId,
    ) -> Result<HttpRequest, WorkerRequestError> {
        self.request(
            Method::GET,
            self.routes.function_version_execution_status,
            &FunctionVersionParam {
                function_version: function_version_id.clone(),
            },
            HashMap::new(),
            true,
        )
    }
}
