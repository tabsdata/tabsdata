//
// Copyright 2026 Tabsdata Inc.
//

use crate::metrics::inflight_response;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{Project, ViewProject};
use td_database::access::{AcrossProjects, GatedConnection, InProject, RO};
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::metrics::{InflightRequest, InflightResponse};
use td_objects::types::visible_projects::VisibleProjects;

/// `GET /inflight` — point-in-time count of in-flight LFRs broken
/// out by `(collection_id, group_id, status)`. Inflight is defined by
/// status, not by `ended_on IS NULL`. RLS filters to the request's project.
/// Authorized: the requester must be able to view the project in scope.
#[service_factory]
async fn inflight(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, InProject>,
    request: WithinProject<ReadRequest<InflightRequest>>,
) -> Result<InflightResponse, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, ViewProject>(gated_conn, &ctx, &project)
        .await?;
    inflight_response(&param, &VisibleProjects::All, gated_conn.get(grant)).await
}

/// `GET /inflight` (global) — the same point-in-time aggregation across every
/// project, an `AcrossProjects` read narrowed to the projects the requester
/// may view.
#[service_factory]
async fn inflight_global(
    authz: Ctx<AuthzContext>,
    gated_conn: &mut GatedConnection<RO, AcrossProjects>,
    request: ReadRequest<InflightRequest>,
) -> Result<InflightResponse, TdError> {
    let (ctx, param) = request.into_parts();
    let (grant, visible) = authz.visible_projects(gated_conn, &ctx).await?;
    inflight_response(&param, &visible, gated_conn.get(grant)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::collection::{ActiveCollectionDB, CollectionCreate};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::query_filters::DimensionFilter;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, Description, FunctionName,
        FunctionRuntimeValues, GroupId, GroupName, LogicalFunctionRunStatus, ProjectName,
        TableNameDto, TransactionKey, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_inflight_empty_db(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = InflightRequest {
            collections: DimensionFilter::default(),
            groups: DimensionFilter::default(),
            statuses: vec![],
        };
        let service = Services::with_defaults(db);
        let response = service
            .inflight(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(request)
                    .in_project(p.id),
            )
            .await?
            .data;
        assert!(response.rows.is_empty());
        Ok(())
    }

    /// Cross-tenant isolation on the `/inflight` metrics path. The query carries
    /// no project predicate and leans entirely on the per-request GUC + RLS, so a
    /// request scoped to a different project must observe none of another
    /// project's in-flight runs.
    /// We seed an in-flight run scoped to one project and confirm a
    /// freshly-created empty project cannot see it.
    #[td_test::test(db)]
    async fn test_inflight_isolated_by_project(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;

        // A second, empty project (its own system groups, no runs).
        let other_project = db
            .seed()
            .project(ProjectName::try_from("inflight_iso_b")?)
            .await?;

        // An in-flight run in the first project.
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("inflight_iso_coll")?,
                &UserId::admin(),
            )
            .await?;
        let create = FunctionRegister {
            name: FunctionName::try_from("inflight_iso_fn")?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("t0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        db.scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Running,
            )
            .await?;

        // Scoped to the first project: sees its own in-flight run.
        let in_test = Services::with_defaults(db.clone())
            .inflight(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(InflightRequest {
                        collections: DimensionFilter::default(),
                        groups: DimensionFilter::default(),
                        statuses: vec![],
                    })
                    .in_project(p.id),
            )
            .await?
            .data;
        assert!(
            !in_test.rows.is_empty(),
            "the first project must see its own in-flight run",
        );

        // Scoped to the other project: RLS hides the first project's run.
        let other = Services::with_defaults(db.clone())
            .inflight(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .read(InflightRequest {
                        collections: DimensionFilter::default(),
                        groups: DimensionFilter::default(),
                        statuses: vec![],
                    })
                    .in_project(other_project.id),
            )
            .await?
            .data;
        assert!(
            other.rows.is_empty(),
            "a different project must not see the first project's in-flight runs",
        );
        Ok(())
    }

    /// Seed one Running (in-flight) function run in the given collection — the
    /// run inherits the collection's project and group through the seed chain.
    async fn seed_inflight_run(
        db: &TdPool,
        collection: &ActiveCollectionDB,
    ) -> Result<(), TdError> {
        let create = FunctionRegister {
            name: FunctionName::try_from("g_inflight_fn")?,
            description: "d".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "s".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("t0")?].into(),
            table_types: None,
            runtime_values: FunctionRuntimeValues::try_from("rv")?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
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
    /// multiply the counts. Bypass-vs-RLS is the whole point.
    #[td_test::test(db)]
    async fn test_inflight_global_spans_and_attributes_projects(db: TdPool) -> Result<(), TdError> {
        // Project A: an in-flight run in one of its collections.
        let p = db.seed().test_project().await?;
        let coll_a = db
            .scoped(p.id)
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
        Services::with_defaults(db.clone())
            .create_collection(
                RequestContext::with(AccessTokenId::default(), UserId::admin())
                    .create(
                        (),
                        CollectionCreate {
                            name: coll_b_name.clone(),
                            description: Description::try_from("d")?,
                            group: GroupName::default_group(),
                            base_path: None,
                            connection: None,
                            secrets: None,
                            vars: None,
                        },
                    )
                    .in_project(project_b.id),
            )
            .await?;
        let coll_b: ActiveCollectionDB = db
            .scoped(&project_b_id)
            .fetch_one::<ActiveCollectionDB>(&coll_b_name)
            .await?;
        seed_inflight_run(&db, &coll_b).await?;

        // The global request is project-free (bare InflightRequest) and reads
        // across projects, so both projects' runs land in one response.
        let response = Services::with_defaults(db.clone())
            .inflight_global(
                RequestContext::with(AccessTokenId::default(), UserId::admin()).read(
                    InflightRequest {
                        collections: DimensionFilter::default(),
                        groups: DimensionFilter::default(),
                        statuses: vec![],
                    },
                ),
            )
            .await?
            .data;

        let row_a = response
            .rows
            .iter()
            .find(|r| r.collection_id == coll_a.id)
            .expect("project A collection's in-flight run must appear in the global view");
        let row_b = response
            .rows
            .iter()
            .find(|r| r.collection_id == coll_b.id)
            .expect("project B collection's in-flight run must appear in the global view");

        // Each row is attributed to its own project.
        assert_eq!(row_a.project_id, p.id);
        assert_eq!(row_b.project_id, project_b.id);
        // Both `default` groups share the fixed id; the project tag disambiguates.
        assert_eq!(row_a.group_id, coll_a.group_id);
        assert_eq!(row_b.group_id, coll_b.group_id);
        assert_eq!(coll_a.group_id, GroupId::default_group());
        assert_eq!(coll_b.group_id, GroupId::default_group());
        // A groups join not matching project_id would fan out and double these.
        assert_eq!(row_a.count, 1, "fan-out: project A count inflated");
        assert_eq!(row_b.count, 1, "fan-out: project B count inflated");
        Ok(())
    }
}
