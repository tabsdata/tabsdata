//
// Copyright 2025 Tabsdata Inc.
//

use std::time::Instant;
use td_error::TdError;

pub use tm_services::{FieldAccessors, service_factory};

/// Reads one dependency out of the services struct. `#[derive(FieldAccessors)]`
/// writes one impl per field, so the field's *type* is the key — two fields of
/// the same type are indistinguishable here.
#[diagnostic::on_unimplemented(
    message = "`{T}` holds no `{Self}` field",
    note = "add one to `{T}` — `#[derive(FieldAccessors)]` keys on the field's type, not its name"
)]
pub trait FieldAccessor<T> {
    fn get_field(ctx: &T) -> Self;
}

/// A service parameter the body owns: built from the services struct and the
/// request, moved in, nothing to end. One the wrapper ends implements
/// [`ExtractMut`] instead.
///
/// Both are implemented on the type as *declared*, so a parameter whose form
/// does not match its type is a missing impl at the parameter rather than
/// something that compiles and misbehaves.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a service extractor for `{Srv}`",
    note = "a parameter the body owns implements `Extract`; one the wrapper ends when the \
            service returns is declared `&mut T` and implements `ExtractMut`"
)]
pub trait Extract<Srv, Req>: Sized {
    fn extract(srv: &Srv, req: &Req) -> impl Future<Output = Result<Self, TdError>> + Send;
}

/// One run of a service by its wrapper.
#[derive(Debug, Clone, Copy)]
pub struct Pass {
    /// The service, for logs.
    pub unit: &'static str,
    /// Starting at 1.
    pub number: u32,
    /// When the first run began.
    pub started: Instant,
}

impl Pass {
    pub fn first(unit: &'static str) -> Self {
        Self {
            unit,
            number: 1,
            started: Instant::now(),
        }
    }

    pub fn next(self) -> Self {
        Self {
            number: self.number + 1,
            ..self
        }
    }
}

/// What ending a parameter leaves the wrapper to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finished {
    Done,
    /// Run the whole service again; the parameter already waited out any
    /// backoff.
    Retry,
}

/// A service parameter the wrapper owns: extracted for the body to borrow, and
/// ended once the body returns — how a connection commits on success and rolls
/// back on error with no service body involved, and asks for the service to
/// run again when the database aborted it. Implemented on `&mut T`, so it
/// cannot be taken by value and left unended.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a managed service extractor for `{Srv}`",
    note = "the wrapper ends a `&mut T` parameter, so `T` must have something to end; a \
            parameter with nothing to end is declared by value and implements `Extract`"
)]
pub trait ExtractMut<Srv, Req>: Sized {
    /// What the wrapper keeps while the body runs; `Self` borrows it.
    type Kept: Send;

    fn extract(srv: &Srv, req: &Req) -> impl Future<Output = Result<Self::Kept, TdError>> + Send;

    /// End what was kept, given how the body ended.
    fn finish(
        kept: Self::Kept,
        outcome: Result<(), &TdError>,
        pass: Pass,
    ) -> impl Future<Output = Result<Finished, TdError>> + Send;
}

/// Ends one parameter after a failed extraction of a later one: nothing ran,
/// so the ending is a rollback whose own failure is only logged — the
/// extraction error is the one the caller must see.
pub async fn abandon_param<P, Srv, Req>(kept: P::Kept, error: &TdError, pass: Pass)
where
    P: ExtractMut<Srv, Req>,
{
    if let Err(e) = P::finish(kept, Err(error), pass).await {
        tracing::warn!(error = %e, "service: ending a parameter failed");
    }
}

/// Ends one parameter, folded into the service's outcome: a failed ending
/// becomes the request's error, unless the body already failed. `retry` is
/// set when any ended parameter asks for the service to run again.
pub async fn finish_param<P, Srv, Req, R>(
    kept: P::Kept,
    out: Result<R, TdError>,
    pass: Pass,
    retry: &mut bool,
) -> Result<R, TdError>
where
    P: ExtractMut<Srv, Req>,
{
    let finished = P::finish(kept, out.as_ref().map(|_| ()), pass).await;
    match finished {
        Ok(Finished::Done) => out,
        Ok(Finished::Retry) => {
            *retry = true;
            out
        }
        Err(e) if out.is_ok() => Err(e),
        Err(e) => {
            tracing::warn!(error = %e, "service: ending a parameter failed");
            out
        }
    }
}
