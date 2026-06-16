//
// Copyright 2026 Tabs Data Inc.
//

//! Project DXOs (Data Exchange Objects) — mirrors the collection DXOs but
//! one rung "above" collections: projects are the tenant boundary for
//! row-level security and are themselves a global registry (not RLS-scoped).

#[ta_objects::dxo]
mod definitions {
    use crate::dxo::crudl::RequestContext;
    use crate::types::basic::{AtTime, Description, ProjectId, ProjectName, UserId, UserName};
    use td_common::id::Id;

    #[ta_objects::Dao]
    #[dao(sql_table = "projects")]
    #[dxo(
        builder(try_from = ProjectCreate, skip_all),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct ProjectCreateDB {
        #[dxo(extractor)]
        #[builder(default)]
        pub id: ProjectId,
        #[dxo(builder(include), extractor)]
        pub name: ProjectName,
        #[dxo(builder(include))]
        pub description: Description,
        #[dxo(updater(include, field = "time"))]
        pub created_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub created_by_id: UserId,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
        #[builder(default)]
        pub name_when_deleted: Option<ProjectName>,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "projects_active")]
    #[dxo(builder(try_from = ProjectCreateDB))]
    pub struct ProjectDB {
        #[dxo(extractor)]
        pub id: ProjectId,
        #[dxo(extractor)]
        pub name: ProjectName,
        pub description: Description,
        pub created_on: AtTime,
        pub created_by_id: UserId,
        pub modified_on: AtTime,
        pub modified_by_id: UserId,
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "projects")]
    #[dxo(
        builder(try_from = ProjectDB, skip_all),
        updater(try_from = RequestContext, skip_all),
    )]
    pub struct ProjectDeleteDB {
        #[builder(default = "generate_deleted_name()")]
        pub name: ProjectName,

        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,

        #[dxo(builder(include, field = "name"))]
        pub name_when_deleted: ProjectName,
    }

    fn generate_deleted_name() -> ProjectName {
        ProjectName::try_from(format!("deleted_{}", Id::default())).unwrap()
    }

    #[ta_objects::Dao]
    #[dao(sql_table = "projects__with_names")]
    #[inherits(ProjectDB)]
    pub struct ProjectDBWithNames {
        #[dxo(extractor)]
        pub id: ProjectId,
        pub created_by: UserName,
        pub modified_by: UserName,
    }

    #[ta_objects::Dto]
    pub struct ProjectCreate {
        pub name: ProjectName,
        pub description: Description,
    }

    #[ta_objects::Dto]
    pub struct ProjectUpdate {
        pub name: Option<ProjectName>,
        pub description: Option<Description>,
    }

    #[ta_objects::Dao]
    #[dao(update, sql_table = "projects")]
    #[dxo(
        builder(try_from = ProjectDB),
        updater(try_from = RequestContext, skip_all)
    )]
    pub struct ProjectUpdateDB {
        pub name: ProjectName,
        pub description: Description,
        #[dxo(updater(include, field = "time"))]
        pub modified_on: AtTime,
        #[dxo(updater(include, field = "user_id"))]
        pub modified_by_id: UserId,
    }

    #[ta_objects::Dto]
    #[dto(list(on = ProjectDBWithNames))]
    #[inherits(ProjectDBWithNames)]
    pub struct ProjectRead {
        #[dto(list(pagination_by = "+"))]
        pub id: ProjectId,
        #[dto(list(filter, filter_like, order_by))]
        pub name: ProjectName,
        #[dto(list(filter, filter_like, order_by))]
        pub description: Description,
    }
}
