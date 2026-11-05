//
// Copyright 2026 Tabsdata Inc.
//

//! `Extract` impls for the connection machinery, making `DbPool<..>` and
//! `GatedConnection<M, S, D>` declarable as `#[service_factory]` fn parameters.
//! `Global` and `AcrossProjects` variants extract from any request;
//! `InProject` variants require a project-scoped one ([`ProjectScoped`]), so
//! declaring a project-bound connection on an unscoped request does not
//! compile.

use super::{AcrossProjects, DbPool, GatedConnection, Global, InProject, OpensIn, ProjectKey};
use crate::domain::{DomainPools, ScopedBy};
use crate::pool::scoped::Scoped;
use crate::pool::{RegularPool, ServingPool};
use ta_services::factory::{Extract, ExtractMut, FieldAccessor, Finished, Pass};
use td_error::TdError;

/// A request carrying the project it is scoped to; the source of the
/// `InProject` scope at extraction time.
#[diagnostic::on_unimplemented(
    message = "an `InProject` parameter needs a project-scoped request",
    note = "the service fn's last parameter must be `WithinProject<Request>`"
)]
pub(crate) trait ProjectScoped {
    fn project_scope(&self) -> InProject;
}

impl<R, P: ProjectKey> ProjectScoped for Scoped<R, P> {
    fn project_scope(&self) -> InProject {
        InProject::new(&self.project)
    }
}

impl<Srv, Req, D: ScopedBy<Global>, P: ServingPool> Extract<Srv, Req> for DbPool<Global, P, D>
where
    Srv: Sync,
    Req: Sync,
    DomainPools: FieldAccessor<Srv>,
{
    async fn extract(srv: &Srv, _req: &Req) -> Result<Self, TdError> {
        Ok(DbPool::with(DomainPools::get_field(srv), Global))
    }
}

impl<Srv, Req, D: ScopedBy<AcrossProjects>, P: ServingPool> Extract<Srv, Req>
    for DbPool<AcrossProjects, P, D>
where
    Srv: Sync,
    Req: Sync,
    DomainPools: FieldAccessor<Srv>,
{
    async fn extract(srv: &Srv, _req: &Req) -> Result<Self, TdError> {
        Ok(DbPool::with(DomainPools::get_field(srv), AcrossProjects))
    }
}

impl<Srv, Req, D: ScopedBy<InProject>, P: ServingPool> Extract<Srv, Req> for DbPool<InProject, P, D>
where
    Srv: Sync,
    Req: ProjectScoped + Sync,
    DomainPools: FieldAccessor<Srv>,
{
    async fn extract(srv: &Srv, req: &Req) -> Result<Self, TdError> {
        Ok(DbPool::with(
            DomainPools::get_field(srv),
            req.project_scope(),
        ))
    }
}

impl<Srv, Req, M: OpensIn<Global>, D: ScopedBy<Global>> ExtractMut<Srv, Req>
    for &mut GatedConnection<M, Global, D>
where
    Srv: Sync,
    Req: Sync,
    DomainPools: FieldAccessor<Srv>,
{
    type Kept = GatedConnection<M, Global, D>;

    async fn extract(srv: &Srv, req: &Req) -> Result<Self::Kept, TdError> {
        DbPool::<Global, RegularPool, D>::extract(srv, req)
            .await?
            .open_extracted::<M>()
            .await
    }

    async fn finish(
        kept: Self::Kept,
        outcome: Result<(), &TdError>,
        pass: Pass,
    ) -> Result<Finished, TdError> {
        kept.finish(outcome, pass).await
    }
}

impl<Srv, Req, M: OpensIn<AcrossProjects>, D: ScopedBy<AcrossProjects>> ExtractMut<Srv, Req>
    for &mut GatedConnection<M, AcrossProjects, D>
where
    Srv: Sync,
    Req: Sync,
    DomainPools: FieldAccessor<Srv>,
{
    type Kept = GatedConnection<M, AcrossProjects, D>;

    async fn extract(srv: &Srv, req: &Req) -> Result<Self::Kept, TdError> {
        DbPool::<AcrossProjects, RegularPool, D>::extract(srv, req)
            .await?
            .open_extracted::<M>()
            .await
    }

    async fn finish(
        kept: Self::Kept,
        outcome: Result<(), &TdError>,
        pass: Pass,
    ) -> Result<Finished, TdError> {
        kept.finish(outcome, pass).await
    }
}

impl<Srv, Req, M: OpensIn<InProject>, D: ScopedBy<InProject>> ExtractMut<Srv, Req>
    for &mut GatedConnection<M, InProject, D>
where
    Srv: Sync,
    Req: ProjectScoped + Sync,
    DomainPools: FieldAccessor<Srv>,
{
    type Kept = GatedConnection<M, InProject, D>;

    async fn extract(srv: &Srv, req: &Req) -> Result<Self::Kept, TdError> {
        DbPool::<InProject, RegularPool, D>::extract(srv, req)
            .await?
            .open_extracted::<M>()
            .await
    }

    async fn finish(
        kept: Self::Kept,
        outcome: Result<(), &TdError>,
        pass: Pass,
    ) -> Result<Finished, TdError> {
        kept.finish(outcome, pass).await
    }
}
