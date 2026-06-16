//
// Copyright 2026 Tabs Data Inc.
//

use crate::metrics::layers::{build_inflight_response, build_inflight_response_global};
use ta_services::factory::service_factory;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, Rbac, Scope, ViewProject, Visible};
use td_objects::dxo::crudl::{ReadRequest, RequestContext};
use td_objects::dxo::metrics::{InflightRequest, InflightResponse};
use td_objects::tower_service::with::With;
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

/// `GET /inflight` — point-in-time count of in-flight LFRs broken
/// out by `(collection_id, group_id, status)`. Inflight is defined by
/// status, not by `ended_on IS NULL`. RLS filters to the request's project.
/// Authorized: the requester must be able to view the project in scope.
#[service_factory(
    name = InflightService,
    request = ReadRequest<InflightRequest>,
    response = InflightResponse,
    connection = ConnectionProvider,
    scope = "Project",
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<InflightRequest>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<InflightRequest>>::extract_name::<InflightRequest>),
        // Authorize: the requester must be able to view the project in scope.
        from_fn(Scope::<Project>::set),
        from_fn(Rbac::<Project, ViewProject>::check),
        from_fn(build_inflight_response),
    )
}

/// `GET /inflight` (global) — the same point-in-time aggregation across every
/// project. `scope = "Global"` bypasses RLS so the query spans all tenants;
/// the result is then narrowed to the projects the requester may view
/// (`Visible::projects` + the `*_global` builder).
#[service_factory(
    name = InflightGlobalService,
    request = ReadRequest<InflightRequest>,
    response = InflightResponse,
    connection = ConnectionProvider,
    scope = "Global",
    context = AuthzContext,
)]
fn service_global() {
    layers!(
        from_fn(With::<ReadRequest<InflightRequest>>::extract::<RequestContext>),
        from_fn(With::<ReadRequest<InflightRequest>>::extract_name::<InflightRequest>),
        // Narrow the cross-project aggregation to the requester's viewable projects.
        from_fn(Visible::projects),
        from_fn(build_inflight_response_global),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::collection::service::CollectionServices;
    use ta_services::factory::ServiceFactory;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_database::scoped::InProject;
    use td_database::sea_query::kind::{DatabaseBackend, DatabaseKind};
    use td_error::TdError;
    use td_objects::Buildable;
    use td_objects::dxo::collection::{CollectionCreate, CollectionDB};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::query_filters::DimensionFilter;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionName,
        FunctionRuntimeValues, GroupId, LogicalFunctionRunStatus, ProjectId, ProjectName, RoleId,
        TableNameDto, TransactionKey, UserId,
    };
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;

    #[td_test::test(db)]
    async fn test_inflight_empty_db(db: TdPool) -> Result<(), TdError> {
        let request = InflightRequest {
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let service = InflightService::with_defaults(db).service();
        let response = service
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(ProjectId::sandbox()),
            )
            .await?;
        assert!(response.rows.is_empty());
        Ok(())
    }

    /// Cross-tenant isolation on the `/inflight` metrics path. The query carries
    /// no project predicate and leans entirely on the per-request GUC + RLS, so a
    /// request scoped to a different project must observe none of another
    /// project's in-flight runs. Postgres-only (SQLite/MySQL have no RLS).
    /// We seed an in-flight run scoped to `sandbox` and confirm a freshly-created
    /// empty project cannot see it.
    #[td_test::test(db)]
    async fn test_inflight_isolated_by_project(db: TdPool) -> Result<(), TdError> {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }

        // A second, empty project (its own system groups, no runs).
        let other_project = db
            .seed()
            .project(ProjectName::try_from("inflight_iso_b")?)
            .await?;

        // An in-flight run in `sandbox`.
        let collection = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(
                &CollectionName::try_from("inflight_iso_coll")?,
                &UserId::admin(),
            )
            .await?;
        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("inflight_iso_fn")?)
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("t0")?])
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let function = db
            .scoped(ProjectId::sandbox())
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db
            .scoped(ProjectId::sandbox())
            .seed()
            .execution(&function)
            .await?;
        let transaction = db
            .scoped(ProjectId::sandbox())
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        db.scoped(ProjectId::sandbox())
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Running,
            )
            .await?;

        // Scoped to sandbox: sees its own in-flight run.
        let sandbox = InflightService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(InflightRequest {
                        collections: DimensionFilter::default(),
                        groups: DimensionFilter::default(),
                        statuses: vec![],
                    })
                    .in_project(ProjectId::sandbox()),
            )
            .await?;
        assert!(
            !sandbox.rows.is_empty(),
            "sandbox must see its own in-flight run",
        );

        // Scoped to the other project: RLS hides sandbox's run.
        let other = InflightService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(InflightRequest {
                        collections: DimensionFilter::default(),
                        groups: DimensionFilter::default(),
                        statuses: vec![],
                    })
                    .in_project(other_project.id),
            )
            .await?;
        assert!(
            other.rows.is_empty(),
            "a different project must not see sandbox's in-flight runs",
        );
        Ok(())
    }

    /// Seed one Running (in-flight) function run in the given collection — the
    /// run inherits the collection's project and group through the seed chain.
    async fn seed_inflight_run(db: &TdPool, collection: &CollectionDB) -> Result<(), TdError> {
        let create = FunctionRegister::builder()
            .name(FunctionName::try_from("g_inflight_fn")?)
            .try_description("d")?
            .bundle_id(BundleId::default())
            .try_snippet("s")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("t0")?])
            .runtime_values(FunctionRuntimeValues::try_from("rv")?)
            .reuse_frozen_tables(false)
            .build()?;
        let p = &collection.project_id;
        let function = db.scoped(p).seed().function(collection, &create).await?;
        let execution = db.scoped(p).seed().execution(&function).await?;
        let transaction = db
            .scoped(p)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        db.scoped(p)
            .seed()
            .physical_function_run(
                collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Running,
            )
            .await?;
        Ok(())
    }

    /// The global `/inflight` service aggregates across every project (bypass
    /// RLS), attributing each row to its owning project. System groups carry
    /// the same fixed id in every project, so this doubles as the regression
    /// pin for bypass fan-out: a groups join not matching project_id would
    /// multiply the counts. Postgres-only (bypass-vs-RLS is the whole point).
    #[td_test::test(db)]
    async fn test_inflight_global_spans_and_attributes_projects(db: TdPool) -> Result<(), TdError> {
        if !matches!(db.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }

        // Project A = sandbox: an in-flight run in a sandbox collection.
        let coll_a = db
            .scoped(ProjectId::sandbox())
            .seed()
            .collection(&CollectionName::try_from("g_inflight_a")?, &UserId::admin())
            .await?;
        seed_inflight_run(&db, &coll_a).await?;

        // Project B: a fresh project with its own collection — created through
        // the service so it resolves B's own `default` group — plus a run.
        let project_b = db
            .seed()
            .project(ProjectName::try_from("g_inflight_b")?)
            .await?;
        let coll_b_name = CollectionName::try_from("g_inflight_b_coll")?;
        let project_b_id = project_b.id.to_string();
        CollectionServices::build(&Context::with_defaults(db.clone()))
            .create
            .service()
            .raw_oneshot(
                RequestContext::with(
                    AccessTokenId::default(),
                    UserId::admin()
                )
                .create(
                    (),
                    CollectionCreate::builder()
                        .name(&coll_b_name)
                        .description(&Description::try_from("d")?)
                        .build()?,
                )
                .in_project(project_b.id),
            )
            .await?;
        let coll_b: CollectionDB = db
            .scoped(&project_b_id)
            .live()
            .fetch_one::<CollectionDB>(&coll_b_name)
            .await?;
        seed_inflight_run(&db, &coll_b).await?;

        // Global request is project-free (bare InflightRequest) → bypass RLS →
        // both projects' runs are visible in one response.
        let response = InflightGlobalService::with_defaults(db.clone())
            .service()
            .raw_oneshot(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
                    InflightRequest {
                        collections: DimensionFilter::default(),
                        groups: DimensionFilter::default(),
                        statuses: vec![],
                    },
                ),
            )
            .await?;

        let row_a = response
            .rows
            .iter()
            .find(|r| r.collection_id == coll_a.id)
            .expect("sandbox collection's in-flight run must appear in the global view");
        let row_b = response
            .rows
            .iter()
            .find(|r| r.collection_id == coll_b.id)
            .expect("project B collection's in-flight run must appear in the global view");

        // Each row is attributed to its own project.
        assert_eq!(row_a.project_id, ProjectId::sandbox());
        assert_eq!(row_b.project_id, project_b.id);
        // Both `default` groups share the fixed id; the project tag disambiguates.
        assert_eq!(row_a.group_id, coll_a.group_id);
        assert_eq!(row_b.group_id, coll_b.group_id);
        assert_eq!(coll_a.group_id, GroupId::default_group());
        assert_eq!(coll_b.group_id, GroupId::default_group());
        // A groups join not matching project_id would fan out and double these.
        assert_eq!(row_a.count, 1, "fan-out: sandbox count inflated");
        assert_eq!(row_b.count, 1, "fan-out: project B count inflated");
        Ok(())
    }
}
