//
// Copyright 2026 Tabsdata Inc.
//

//! The parameter lifecycle the `#[service_factory]` wrapper implements, with no
//! database in the way. `Resource` is declarable only as `&mut Resource` and
//! `Handle` only by value, since each implements one of the two traits.

// The generated code names `::ta_services::factory::*`, which in this crate's
// own test build is the self dev-dependency, so the impls below target it.
use std::sync::{Arc, Mutex};
use ta_services::factory::{Extract, ExtractMut, service_factory};
use td_error::{TdError, td_error};

#[td_error]
enum ProbeError {
    #[error("the body failed")]
    BodyFailed = 0,
    #[error("the ending failed")]
    EndingFailed = 1,
    #[error("the extraction failed")]
    ExtractionFailed = 2,
}

#[derive(Default, Clone)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn record(&self, event: impl Into<String>) {
        self.0.lock().unwrap().push(event.into());
    }

    fn events(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

struct Probe {
    log: Log,
    ending_fails: bool,
}

impl Probe {
    fn new(ending_fails: bool) -> Self {
        Self {
            log: Log::default(),
            ending_fails,
        }
    }
}

/// Stands in for a connection: extracted per request, ended by the wrapper.
struct Resource {
    log: Log,
    ending_fails: bool,
}

impl Resource {
    fn use_it(&mut self) {
        self.log.record("body");
    }
}

/// A second kept resource whose extraction always fails, and a third one
/// tagged `second`, so extraction-failure cleanup and ending order are
/// observable per parameter.
struct FailingResource;

impl<Req: Sync> ExtractMut<Probe, Req> for &mut FailingResource {
    type Kept = FailingResource;

    async fn extract(srv: &Probe, _req: &Req) -> Result<Self::Kept, TdError> {
        srv.log.record("extract failing");
        Err(ProbeError::ExtractionFailed.into())
    }

    async fn finish(_kept: Self::Kept, _ok: bool) -> Result<(), TdError> {
        unreachable!("never extracted, never ended")
    }
}

struct SecondResource {
    log: Log,
}

impl SecondResource {
    fn use_it(&mut self) {
        self.log.record("body second");
    }
}

impl<Req: Sync> ExtractMut<Probe, Req> for &mut SecondResource {
    type Kept = SecondResource;

    async fn extract(srv: &Probe, _req: &Req) -> Result<Self::Kept, TdError> {
        srv.log.record("extract second");
        Ok(SecondResource {
            log: srv.log.clone(),
        })
    }

    async fn finish(kept: Self::Kept, ok: bool) -> Result<(), TdError> {
        kept.log.record(format!("finish second({ok})"));
        Ok(())
    }
}

/// Ended by the wrapper, so it is declarable only as `&mut Resource`.
impl<Req: Sync> ExtractMut<Probe, Req> for &mut Resource {
    type Kept = Resource;

    async fn extract(srv: &Probe, _req: &Req) -> Result<Self::Kept, TdError> {
        srv.log.record("extract");
        Ok(Resource {
            log: srv.log.clone(),
            ending_fails: srv.ending_fails,
        })
    }

    async fn finish(kept: Self::Kept, ok: bool) -> Result<(), TdError> {
        kept.log.record(format!("finish({ok})"));
        if kept.ending_fails {
            return Err(ProbeError::EndingFailed.into());
        }
        Ok(())
    }
}

/// Nothing to end, so it is declarable only by value.
struct Handle {
    log: Log,
}

impl Handle {
    fn use_it(&mut self) {
        self.log.record("body");
    }
}

impl<Req: Sync> Extract<Probe, Req> for Handle {
    async fn extract(srv: &Probe, _req: &Req) -> Result<Self, TdError> {
        srv.log.record("extract");
        Ok(Handle {
            log: srv.log.clone(),
        })
    }
}

#[service_factory(on = Probe)]
async fn succeeds(resource: &mut Resource, _request: ()) -> Result<u8, TdError> {
    resource.use_it();
    Ok(7)
}

#[service_factory(on = Probe)]
async fn fails(resource: &mut Resource, _request: ()) -> Result<u8, TdError> {
    resource.use_it();
    Err(ProbeError::BodyFailed.into())
}

#[service_factory(on = Probe)]
async fn owns_it(mut handle: Handle, _request: ()) -> Result<u8, TdError> {
    handle.use_it();
    Ok(7)
}

#[service_factory(on = Probe)]
async fn second_extraction_fails(
    resource: &mut Resource,
    failing: &mut FailingResource,
    _request: (),
) -> Result<u8, TdError> {
    resource.use_it();
    let _ = failing;
    unreachable!("the second extraction fails")
}

#[service_factory(on = Probe)]
async fn two_kept(
    resource: &mut Resource,
    second: &mut SecondResource,
    _request: (),
) -> Result<u8, TdError> {
    resource.use_it();
    second.use_it();
    Ok(7)
}

#[tokio::test]
async fn test_body_ok_ends_with_ok() {
    let probe = Probe::new(false);
    let response = probe.succeeds(()).await.unwrap();

    assert_eq!(response.data, 7);
    assert_eq!(probe.log.events(), ["extract", "body", "finish(true)"]);
}

#[tokio::test]
async fn test_body_error_ends_with_err() {
    let probe = Probe::new(false);
    let error = probe.fails(()).await.expect_err("the body fails");

    assert_eq!(error.code(), "ProbeError::0000");
    assert_eq!(probe.log.events(), ["extract", "body", "finish(false)"]);
}

/// A failed ending is the request's error: the body succeeded but its work did
/// not become durable, so the response must not say it did.
#[tokio::test]
async fn test_failed_ending_becomes_the_error() {
    let probe = Probe::new(true);
    let error = probe.succeeds(()).await.expect_err("the ending fails");

    assert_eq!(error.code(), "ProbeError::0001");
}

/// A failed ending never masks what the body returned.
#[tokio::test]
async fn test_failed_ending_does_not_mask_the_body_error() {
    let probe = Probe::new(true);
    let error = probe.fails(()).await.expect_err("the body fails");

    assert_eq!(error.code(), "ProbeError::0000");
}

/// A failed later extraction ends the parameters already kept — the earlier
/// connection is rolled back awaited, not left to the drop-queued rollback —
/// and the extraction error is what surfaces.
#[tokio::test]
async fn test_failed_extraction_ends_already_kept_params() {
    let probe = Probe::new(false);
    let error = probe
        .second_extraction_fails(())
        .await
        .expect_err("the second extraction fails");

    assert_eq!(error.code(), "ProbeError::0002");
    assert_eq!(
        probe.log.events(),
        ["extract", "extract failing", "finish(false)"]
    );
}

/// Endings run newest-first, the reverse of acquisition.
#[tokio::test]
async fn test_kept_params_end_in_reverse_order() {
    let probe = Probe::new(false);
    probe.two_kept(()).await.unwrap();

    assert_eq!(
        probe.log.events(),
        [
            "extract",
            "extract second",
            "body",
            "body second",
            "finish second(true)",
            "finish(true)",
        ]
    );
}

/// Taken by value, the parameter is the body's: the wrapper never ends it.
#[tokio::test]
async fn test_by_value_parameter_is_not_ended() {
    let probe = Probe::new(false);
    probe.owns_it(()).await.unwrap();

    assert_eq!(probe.log.events(), ["extract", "body"]);
}
