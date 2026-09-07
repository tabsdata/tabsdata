//
// Copyright 2025 Tabsdata Inc.
//

use crate::factory::{Extract, FieldAccessor};
use td_error::TdError;

/// Injected service context: a shared dependency declared as a `Ctx<T>`
/// parameter of a service fn and held as a `Ctx<T>` field of the Services
/// struct. Cheap to clone (an `Arc` inside), read-only via `Deref`.
pub struct Ctx<T>(std::sync::Arc<T>);

impl<Srv, Req, T> Extract<Srv, Req> for Ctx<T>
where
    Srv: Sync,
    Req: Sync,
    T: Send + Sync,
    Ctx<T>: FieldAccessor<Srv>,
{
    async fn extract(srv: &Srv, _req: &Req) -> Result<Self, TdError> {
        Ok(Ctx::get_field(srv))
    }
}

impl<T> Ctx<T> {
    pub fn new(value: T) -> Self {
        Self(std::sync::Arc::new(value))
    }
}

impl<T> std::ops::Deref for Ctx<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> Clone for Ctx<T> {
    fn clone(&self) -> Self {
        Ctx(self.0.clone())
    }
}
