//
// Copyright 2026 Tabsdata Inc.
//

use axum::Extension;
use axum::body::Body;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum_extra::extract::Query;
use futures::TryStreamExt;
use headers::{HeaderMapExt, LastModified};
use http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_MATCH};
use http::{HeaderMap, HeaderValue, StatusCode};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::SystemTime;
use ta_apiserver::status::error_status::ErrorStatus;
use td_apiforge::apiserver_path;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::ui_data::{UiDataPrecondition, UiDataPut, UiDataWrite, UserUiDataDB};
use td_objects::types::basic::{UiDataDocument, UiDataEtag};
use td_services::Services;
use td_services::ui_data::UiDataError;
use utoipa::openapi::content::ContentBuilder;
use utoipa::openapi::header::HeaderBuilder;
use utoipa::openapi::response::ResponseBuilder;
use utoipa::openapi::schema::{ObjectBuilder, Schema, SchemaType, Type};
use utoipa::openapi::{RefOr, response};
use utoipa::{IntoResponses, PartialSchema, ToSchema};

const UI_DATA_TAG: &str = "UI Data";
const APPLICATION_JSON: &str = "application/json";
const MAX_DOCUMENT_BYTES: usize = 256 * 1024;

#[derive(Debug, serde::Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct UiDataPutParams {
    /// `true` skips the `If-Match` check and writes unconditionally (last
    /// write wins); `false` by default. Any other value is rejected.
    #[serde(rename = "override")]
    force: Option<String>,
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(get),
    path = "/user/ui-data",
    tag(UI_DATA_TAG),
    doc = "Get the caller's UI data document, unchanged, with its `ETag`. \
           Conditional request headers are ignored: never answers `304`."
)]
pub async fn get_ui_data(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
) -> Result<UiDataFound, ErrorStatus> {
    let row = state.get_ui_data(context).await?.data;
    Ok(UiDataFound(row))
}

#[apiserver_path(
    version(v1),
    auth(session),
    method(put),
    path = "/user/ui-data",
    tag(UI_DATA_TAG),
    doc = "Replace the caller's UI data document: a JSON body of at most 256 KiB, \
           stored unparsed. Send the `ETag` of the last read in `If-Match` to \
           replace it, or no `If-Match` to create it; `override=true` writes \
           unconditionally. A `412` carries the current document and its `ETag`."
)]
pub async fn put_ui_data(
    State(state): State<Services>,
    Extension(context): Extension<RequestContext>,
    Query(params): Query<UiDataPutParams>,
    headers: HeaderMap,
    body: Body,
) -> Result<UiDataWritten, ErrorStatus> {
    let force = parse_override(params.force.as_deref())?;
    require_json(&headers)?;
    let precondition = parse_if_match(&headers)?;
    let data = read_document(&headers, body).await?;
    let request = context.update(
        (),
        UiDataPut {
            data,
            precondition,
            force,
        },
    );
    let write = state.put_ui_data(request).await?.data;
    Ok(UiDataWritten(write))
}

fn parse_override(value: Option<&str>) -> Result<bool, TdError> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(other) => Err(UiDataError::InvalidOverride(other.to_string()))?,
    }
}

fn require_json(headers: &HeaderMap) -> Result<(), TdError> {
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    match media_type {
        Some(media_type) if media_type.eq_ignore_ascii_case(APPLICATION_JSON) => Ok(()),
        _ => Err(api_error!(
            status = StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json"
        )),
    }
}

/// Only the canonical form this server issues can match: a strong ETag is
/// compared byte for byte.
fn parse_if_match(headers: &HeaderMap) -> Result<UiDataPrecondition, TdError> {
    let mut values = headers.get_all(IF_MATCH).iter();
    let Some(value) = values.next() else {
        return Ok(UiDataPrecondition::Absent);
    };
    let value = value.to_str().unwrap_or_default().trim();
    if values.next().is_some() || value == "*" || value.contains(',') {
        Err(UiDataError::UnsupportedIfMatch)?
    }
    let etag = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .and_then(|value| {
            UiDataEtag::from_str(value)
                .ok()
                .filter(|etag| etag.to_string() == value)
        });
    Ok(etag.map_or(UiDataPrecondition::NeverMatches, UiDataPrecondition::Etag))
}

async fn read_document(headers: &HeaderMap, body: Body) -> Result<UiDataDocument, TdError> {
    let declared = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|length| length > MAX_DOCUMENT_BYTES as u64) {
        return Err(too_large());
    }

    let mut bytes = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.try_next().await.map_err(|e| {
        api_error!(
            ApiError::InputError,
            "Could not read the request body: {}",
            e
        )
    })? {
        if bytes.len() + chunk.len() > MAX_DOCUMENT_BYTES {
            return Err(too_large());
        }
        bytes.extend_from_slice(&chunk);
    }

    if bytes.is_empty() {
        Err(UiDataError::EmptyDocument)?
    }
    let text = String::from_utf8(bytes).map_err(|_| UiDataError::DocumentNotUtf8)?;
    if text.contains('\0') {
        Err(UiDataError::DocumentHasNul)?
    }
    UiDataDocument::try_from(text)
}

fn too_large() -> TdError {
    api_error!(
        status = StatusCode::PAYLOAD_TOO_LARGE,
        "The UI data document exceeds {} bytes",
        MAX_DOCUMENT_BYTES
    )
}

pub struct UiDataFound(UserUiDataDB);

impl IntoResponse for UiDataFound {
    fn into_response(self) -> Response {
        let mut response = document_response(StatusCode::OK, &self.0);
        // Without it the browser revalidates on its own with If-None-Match
        // and may answer from its cache instead of fetching.
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

pub struct UiDataWritten(UiDataWrite);

impl IntoResponse for UiDataWritten {
    fn into_response(self) -> Response {
        match self.0 {
            UiDataWrite::Created(row) => version_response(StatusCode::CREATED, &row),
            UiDataWrite::Replaced(row) => version_response(StatusCode::NO_CONTENT, &row),
            UiDataWrite::PreconditionFailed(Some(row)) => {
                document_response(StatusCode::PRECONDITION_FAILED, &row)
            }
            UiDataWrite::PreconditionFailed(None) => {
                StatusCode::PRECONDITION_FAILED.into_response()
            }
            UiDataWrite::PreconditionRequired(etag) => {
                let mut response = ErrorStatus::from(api_error!(
                    status = StatusCode::PRECONDITION_REQUIRED,
                    "If-Match header is required to update an existing document; \
                     use ?override=true to force"
                ))
                .into_response();
                response.headers_mut().insert(ETAG, etag_header(&etag));
                response
            }
        }
    }
}

fn etag_header(etag: &UiDataEtag) -> HeaderValue {
    HeaderValue::from_str(&format!("\"{etag}\"")).expect("a UUID is a valid header value")
}

fn version_response(status: StatusCode, row: &UserUiDataDB) -> Response {
    let mut response = status.into_response();
    let headers = response.headers_mut();
    headers.insert(ETAG, etag_header(&row.etag));
    headers.typed_insert(LastModified::from(SystemTime::from(***row.modified_on)));
    response
}

fn document_response(status: StatusCode, row: &UserUiDataDB) -> Response {
    let mut response = version_response(status, row);
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(APPLICATION_JSON));
    *response.body_mut() = Body::from(row.data.to_string());
    response
}

fn any_json() -> RefOr<Schema> {
    ObjectBuilder::new()
        .description(Some("The client's JSON document, any shape"))
        .into()
}

fn header_doc(description: &str) -> utoipa::openapi::header::Header {
    HeaderBuilder::new()
        .schema(Some(
            ObjectBuilder::new().schema_type(SchemaType::new(Type::String)),
        ))
        .description(Some(description))
        .build()
}

fn response_doc(description: &str, document: bool, version: bool) -> RefOr<response::Response> {
    let mut builder = ResponseBuilder::new().description(description);
    if document {
        builder = builder.content(
            APPLICATION_JSON,
            ContentBuilder::new().schema(Some(any_json())).build(),
        );
    }
    if version {
        builder = builder
            .header("ETag", header_doc("The document's version, a quoted UUID"))
            .header(
                "Last-Modified",
                header_doc("When the document was last written"),
            );
    }
    builder.build().into()
}

impl PartialSchema for UiDataFound {
    fn schema() -> RefOr<Schema> {
        any_json()
    }
}

impl ToSchema for UiDataFound {
    fn name() -> Cow<'static, str> {
        "UiData".into()
    }
}

impl IntoResponses for UiDataFound {
    fn responses() -> BTreeMap<String, RefOr<response::Response>> {
        BTreeMap::from([(
            "200".to_string(),
            response_doc("The stored document, unchanged", true, true),
        )])
    }
}

impl PartialSchema for UiDataWritten {
    fn schema() -> RefOr<Schema> {
        any_json()
    }
}

impl ToSchema for UiDataWritten {
    fn name() -> Cow<'static, str> {
        "UiDataWritten".into()
    }
}

impl IntoResponses for UiDataWritten {
    fn responses() -> BTreeMap<String, RefOr<response::Response>> {
        BTreeMap::from([
            (
                "201".to_string(),
                response_doc("No document existed; it was created", false, true),
            ),
            (
                "204".to_string(),
                response_doc("The document was replaced", false, true),
            ),
            (
                "412".to_string(),
                response_doc(
                    "If-Match does not match: the current document and its version, \
                     or an empty body if none exists",
                    true,
                    true,
                ),
            ),
            (
                "428".to_string(),
                ResponseBuilder::new()
                    .description("A document exists and If-Match is absent: its current ETag")
                    .header("ETag", header_doc("The current version, a quoted UUID"))
                    .build()
                    .into(),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::to_bytes;
    use axum::extract::Request;
    use http::Method;
    use ta_apiserver::router::{RouterStates, Routes};
    use td_database::pool::TdPool;
    use td_objects::types::basic::{AccessTokenId, AtTime, UserEnabled, UserId, UserName};
    use td_queries::test_utils::SeedEntry;
    use tower::ServiceExt;

    const ETAG_VALUE: &str = "0b8e6a52-6f1d-4c3e-9a57-2f0c1d7e4b9a";

    fn router(services: Services, user: UserId) -> Router {
        let states = RouterStates::default().with(services);
        let router: Router = Routes::new(&[])
            .named_router(&["get_ui_data", "put_ui_data"], &states)
            .into();
        router.layer(Extension(RequestContext::with(
            AccessTokenId::default(),
            user,
        )))
    }

    fn put(uri: &str, headers: &[(&str, &str)], body: impl Into<Body>) -> Request {
        let mut builder = Request::builder().method(Method::PUT).uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(body.into()).unwrap()
    }

    fn get() -> Request {
        Request::builder()
            .method(Method::GET)
            .uri(PUT_UI_DATA_PATH)
            .header("If-None-Match", "*")
            .body(Body::empty())
            .unwrap()
    }

    async fn body(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn row(data: &str) -> UserUiDataDB {
        UserUiDataDB {
            user_id: UserId::admin(),
            etag: UiDataEtag::from_str(ETAG_VALUE).unwrap(),
            modified_on: AtTime::default().into(),
            data: UiDataDocument::try_from(data).unwrap(),
        }
    }

    fn if_match(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(IF_MATCH, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn test_parse_override() {
        assert!(!parse_override(None).unwrap());
        assert!(!parse_override(Some("false")).unwrap());
        assert!(parse_override(Some("true")).unwrap());
        assert!(parse_override(Some("TRUE")).is_err());
        assert!(parse_override(Some("")).is_err());
    }

    #[test]
    fn test_require_json() {
        let mut headers = HeaderMap::new();
        assert!(require_json(&headers).is_err());
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(require_json(&headers).is_err());
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(require_json(&headers).is_ok());
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("Application/JSON; charset=utf-8"),
        );
        assert!(require_json(&headers).is_ok());
    }

    #[test]
    fn test_parse_if_match() {
        assert!(matches!(
            parse_if_match(&HeaderMap::new()).unwrap(),
            UiDataPrecondition::Absent
        ));
        assert!(matches!(
            parse_if_match(&if_match(&format!("\"{ETAG_VALUE}\""))).unwrap(),
            UiDataPrecondition::Etag(etag) if etag.to_string() == ETAG_VALUE
        ));
        for unmatchable in [
            ETAG_VALUE.to_string(),
            format!("W/\"{ETAG_VALUE}\""),
            format!("\"{}\"", ETAG_VALUE.to_uppercase()),
            format!("\"{}\"", ETAG_VALUE.replace('-', "")),
            "\"not-a-uuid\"".to_string(),
            "".to_string(),
        ] {
            assert!(
                matches!(
                    parse_if_match(&if_match(&unmatchable)).unwrap(),
                    UiDataPrecondition::NeverMatches
                ),
                "{unmatchable}"
            );
        }
        for unsupported in ["*", "\"a\", \"b\""] {
            assert!(
                parse_if_match(&if_match(unsupported)).is_err(),
                "{unsupported}"
            );
        }
        let mut repeated = if_match(&format!("\"{ETAG_VALUE}\""));
        repeated.append(IF_MATCH, HeaderValue::from_static("\"b\""));
        assert!(parse_if_match(&repeated).is_err());
    }

    #[tokio::test]
    async fn test_read_document() {
        let text = r#"{"b": 1,  "a":[2]}"#;
        let document = read_document(&HeaderMap::new(), Body::from(text))
            .await
            .unwrap();
        assert_eq!(&*document, text);

        let at_limit = "x".repeat(MAX_DOCUMENT_BYTES);
        assert!(
            read_document(&HeaderMap::new(), Body::from(at_limit))
                .await
                .is_ok()
        );
        let over_limit = "x".repeat(MAX_DOCUMENT_BYTES + 1);
        assert!(
            read_document(&HeaderMap::new(), Body::from(over_limit))
                .await
                .is_err()
        );

        let mut declared = HeaderMap::new();
        declared.insert(CONTENT_LENGTH, HeaderValue::from(MAX_DOCUMENT_BYTES + 1));
        assert!(read_document(&declared, Body::from("{}")).await.is_err());

        assert!(
            read_document(&HeaderMap::new(), Body::empty())
                .await
                .is_err()
        );
        assert!(
            read_document(&HeaderMap::new(), Body::from(vec![0xff, 0xfe]))
                .await
                .is_err()
        );
        assert!(
            read_document(&HeaderMap::new(), Body::from("{\"a\":\"\0\"}"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_found_response() {
        let text = r#"{"b": 1,  "a":[2]}"#;
        let response = UiDataFound(row(text)).into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(headers[ETAG], format!("\"{ETAG_VALUE}\"").as_str());
        assert_eq!(headers[CONTENT_TYPE], APPLICATION_JSON);
        assert_eq!(headers[CACHE_CONTROL], "no-store");
        assert!(headers.contains_key(http::header::LAST_MODIFIED));
        assert_eq!(body(response).await, text);
    }

    #[tokio::test]
    async fn test_written_responses() {
        let response = UiDataWritten(UiDataWrite::Created(row("{}"))).into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers()[ETAG],
            format!("\"{ETAG_VALUE}\"").as_str()
        );
        assert!(response.headers().contains_key(http::header::LAST_MODIFIED));
        assert_eq!(body(response).await, "");

        let response = UiDataWritten(UiDataWrite::Replaced(row("{}"))).into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[ETAG],
            format!("\"{ETAG_VALUE}\"").as_str()
        );

        let response =
            UiDataWritten(UiDataWrite::PreconditionFailed(Some(row(r#"{"v":2}"#)))).into_response();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(
            response.headers()[ETAG],
            format!("\"{ETAG_VALUE}\"").as_str()
        );
        assert_eq!(response.headers()[CONTENT_TYPE], APPLICATION_JSON);
        assert_eq!(body(response).await, r#"{"v":2}"#);

        let response = UiDataWritten(UiDataWrite::PreconditionFailed(None)).into_response();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        assert!(!response.headers().contains_key(ETAG));
        assert_eq!(body(response).await, "");

        let etag = UiDataEtag::from_str(ETAG_VALUE).unwrap();
        let response = UiDataWritten(UiDataWrite::PreconditionRequired(etag)).into_response();
        assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(
            response.headers()[ETAG],
            format!("\"{ETAG_VALUE}\"").as_str()
        );
    }

    #[tokio::test]
    async fn test_put_rejected_before_the_service() {
        let json = [("Content-Type", APPLICATION_JSON)];
        let cases = [
            (
                put(&format!("{PUT_UI_DATA_PATH}?override=yes"), &json, "{}"),
                StatusCode::BAD_REQUEST,
            ),
            (
                put(PUT_UI_DATA_PATH, &[("Content-Type", "text/plain")], "{}"),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                put(PUT_UI_DATA_PATH, &[], "{}"),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ),
            (
                put(
                    PUT_UI_DATA_PATH,
                    &[("Content-Type", APPLICATION_JSON), ("If-Match", "*")],
                    "{}",
                ),
                StatusCode::BAD_REQUEST,
            ),
            (put(PUT_UI_DATA_PATH, &json, ""), StatusCode::BAD_REQUEST),
            (
                put(PUT_UI_DATA_PATH, &json, "x".repeat(MAX_DOCUMENT_BYTES + 1)),
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ];
        for (request, status) in cases {
            let uri = request.uri().to_string();
            let response = router(Services::default(), UserId::admin())
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{uri}");
        }
    }

    #[td_test::test(db)]
    async fn test_ui_data_lifecycle(db: TdPool) {
        let user = db
            .seed()
            .user(&UserName::try_from("u0").unwrap(), &UserEnabled::TRUE)
            .await
            .unwrap()
            .id;
        let app = router(Services::with_defaults(db.clone()), user);
        let json = ("Content-Type", APPLICATION_JSON);

        let response = app.clone().oneshot(get()).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let first = r#"{"theme":"dark",  "sidebar":{"collapsed":true}}"#;
        let response = app
            .clone()
            .oneshot(put(PUT_UI_DATA_PATH, &[json], first))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let created = response.headers()[ETAG].to_str().unwrap().to_string();

        let response = app.clone().oneshot(get()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[ETAG], created.as_str());
        assert_eq!(body(response).await, first);

        let response = app
            .clone()
            .oneshot(put(PUT_UI_DATA_PATH, &[json], "{}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(response.headers()[ETAG], created.as_str());

        let second = r#"{"theme":"light"}"#;
        let response = app
            .clone()
            .oneshot(put(
                PUT_UI_DATA_PATH,
                &[json, ("If-Match", &created)],
                second,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let replaced = response.headers()[ETAG].to_str().unwrap().to_string();
        assert_ne!(replaced, created);

        let response = app
            .clone()
            .oneshot(put(PUT_UI_DATA_PATH, &[json, ("If-Match", &created)], "{}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(response.headers()[ETAG], replaced.as_str());
        assert_eq!(body(response).await, second);

        let response = app
            .clone()
            .oneshot(put(
                PUT_UI_DATA_PATH,
                &[json, ("If-Match", "\"not-a-uuid\"")],
                "{}",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

        let response = app
            .clone()
            .oneshot(put(
                &format!("{PUT_UI_DATA_PATH}?override=true"),
                &[json, ("If-Match", &created)],
                "{}",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app.oneshot(get()).await.unwrap();
        assert_eq!(body(response).await, "{}");
    }
}
