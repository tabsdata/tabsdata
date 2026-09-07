//
// Copyright 2025 Tabsdata Inc.
//

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

/// A service parameter the wrapper owns: extracted for the body to borrow, and
/// ended once the body returns — how a connection commits on success and rolls
/// back on error with no service body involved. Implemented on `&mut T`, so it
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

    /// End what was kept; `ok` is whether the body succeeded.
    fn finish(kept: Self::Kept, ok: bool) -> impl Future<Output = Result<(), TdError>> + Send;
}

/// Ends one parameter after a failed extraction of a later one: nothing ran,
/// so the ending is a rollback whose own failure is only logged — the
/// extraction error is the one the caller must see.
pub async fn abandon_param<P, Srv, Req>(kept: P::Kept)
where
    P: ExtractMut<Srv, Req>,
{
    if let Err(e) = P::finish(kept, false).await {
        tracing::warn!(error = %e, "service: ending a parameter failed");
    }
}

/// Ends one parameter, folded into the service's outcome: a failed ending
/// becomes the request's error, unless the body already failed.
pub async fn finish_param<P, Srv, Req, R>(
    kept: P::Kept,
    out: Result<R, TdError>,
) -> Result<R, TdError>
where
    P: ExtractMut<Srv, Req>,
{
    match &out {
        Ok(_) => match P::finish(kept, true).await {
            Ok(()) => out,
            Err(e) => Err(e),
        },
        Err(_) => {
            if let Err(e) = P::finish(kept, false).await {
                tracing::warn!(error = %e, "service: ending a parameter failed");
            }
            out
        }
    }
}
