//
// Copyright 2026 Tabsdata Inc.
//

//! Connection-catalog load cases: the reads a derived index of the catalogs is
//! kept with, over connected collections, their catalogs and their entities,
//! across several projects.

use crate::harness::{analyze, record, timed};
use crate::profile::SizeProfile;
use crate::scaling::assert_subquadratic;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use td_database::access::LockInstant;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::dxo::collection::CollectionInsertDB;
use td_objects::dxo::connection::ConnectionInsertDB;
use td_objects::dxo::connection_catalog::{
    CatalogFilter, CatalogMetadataWriteDB, ConnectionEntityInsertDB,
};
use td_objects::dxo::group::GroupDB;
use td_objects::dxo::project::ProjectDB;
use td_objects::types::basic::{
    AtTime, CatalogEntityKind, CatalogSchemaHash, CatalogTruncated, CatalogUpdatedOn, CheckStatus,
    CheckedOn, CollectionId, CollectionName, CollectionType, CollectionVersionId,
    ConnectionEntityId, ConnectionId, ConnectionKind, Description, EntityCount, EntityDoc,
    EntityFqn, FieldCount, FieldNames, GroupName, MetadataOn, MetadataStatus, ProbeId, ProjectName,
    TextRevision, UserId,
};
use td_objects::types::json::Json;
use td_objects::types::visible_projects::VisibleProjects;
use td_queries::Queries;
use td_queries::test_utils::SeedEntry;

const PROJECTS: usize = 4;
const ENTITIES_PER_CONNECTION: usize = 25;

fn next_salt() -> u64 {
    static SALT: AtomicU64 = AtomicU64::new(0);
    SALT.fetch_add(1, Ordering::Relaxed)
}

fn connections_at(profile: SizeProfile) -> usize {
    match profile {
        SizeProfile::Smoke => 20,
        SizeProfile::Load => 200,
        SizeProfile::Stress => 1_000,
    }
}

/// `connections` connected collections spread over the projects, each with a
/// catalog and its entities; every tenth connection has an `invoices` table.
pub(crate) async fn catalog_dataset(db: &TdPool, connections: usize) -> Result<(), TdError> {
    seed_catalogs(db, connections, ENTITIES_PER_CONNECTION, |i, e| {
        let table = if e == 0 && i % 10 == 0 {
            "invoices".to_owned()
        } else {
            format!("t{e}")
        };
        (table, "id name amount created_on".to_owned())
    })
    .await
}

/// `connections` connected collections spread over the projects, each with
/// a catalog of `entities` entities, `entity(connection, index)` naming each
/// one's table and fields.
async fn seed_catalogs(
    db: &TdPool,
    connections: usize,
    entities: usize,
    entity: impl Fn(usize, usize) -> (String, String),
) -> Result<(), TdError> {
    let existing: Vec<ProjectDB> = db.scd2().fetch_all::<ProjectDB>(()).await?;
    let mut projects = Vec::with_capacity(PROJECTS);
    for i in 0..PROJECTS {
        let name = ProjectName::try_from(format!("catalog_p{i}").as_str())?;
        match existing.iter().find(|p| p.name == name) {
            Some(p) => projects.push(p.project_id),
            None => projects.push(db.seed().project(name).await?.project_id),
        }
    }
    let salt = next_salt();
    let user = UserId::admin();
    let now = AtTime::now();
    for i in 0..connections {
        let scoped = db.scoped(projects[i % PROJECTS]);
        let sources: GroupDB = scoped.scd2().fetch_one(&GroupName::source()).await?;
        let connection_id = ConnectionId::default();
        scoped
            .insert(&ConnectionInsertDB {
                id: connection_id,
                kind: ConnectionKind::try_from("tabsdatak.conn.postgres:PostgresSrcConn")?,
                created_on: now.into(),
                created_by_id: user.into(),
                modified_on: now.into(),
                modified_by_id: user.into(),
            })
            .await?;
        let collection_id = CollectionId::default();
        scoped
            .scd2()
            .open(
                &CollectionInsertDB {
                    id: CollectionVersionId::default(),
                    collection_id,
                    name: CollectionName::try_from(format!("crm_{salt}_{i}").as_str())?,
                    description: Description::default(),
                    base_path: None,
                    group_id: sources.group_id,
                    collection_type: CollectionType::Source,
                    connection_id: Some(connection_id),
                    defined_by_id: user,
                },
                LockInstant::asserted(*now),
            )
            .await?;
        scoped
            .upsert(&CatalogMetadataWriteDB {
                connection_id,
                collection_id,
                kind: ConnectionKind::try_from("tabsdatak.conn.postgres:PostgresSrcConn")?,
                check_status: CheckStatus::Ok,
                check_error: None,
                checked_on: CheckedOn::try_from(now)?,
                schema_hash: CatalogSchemaHash::try_from(format!("hash_{i}").as_str())?,
                entity_count: EntityCount::try_from(entities as i32)?,
                truncated: CatalogTruncated::from(false),
                metadata_on: MetadataOn::try_from(now)?,
                metadata_status: MetadataStatus::Ok,
                metadata_error: None,
                last_probe_id: ProbeId::default(),
                updated_on: CatalogUpdatedOn::try_from(now)?,
            })
            .await?;
        let rows = (0..entities)
            .map(|e| {
                let (table, fields) = entity(i, e);
                Ok(ConnectionEntityInsertDB {
                    id: ConnectionEntityId::default(),
                    connection_id,
                    fqn: EntityFqn::try_from(format!("crm.public.{table}").as_str())?,
                    entity_kind: CatalogEntityKind::try_from("table")?,
                    field_count: FieldCount::try_from(fields.split(' ').count() as i32)?,
                    field_names: FieldNames::try_from(fields.as_str())?,
                    doc: Json(EntityDoc(serde_json::json!({"name": table}))),
                })
            })
            .collect::<Result<Vec<_>, TdError>>()?;
        scoped.chunked().insert(&rows).await?;
    }
    Ok(())
}

/// The visible catalogs and their re-read, then the revision reads, after
/// one text change.
#[td_test::test(db)]
async fn load_connection_catalog_reads(db: TdPool) -> Result<(), TdError> {
    let profile = SizeProfile::from_env();
    let connections = connections_at(profile);
    catalog_dataset(&db, connections).await?;
    let entities = connections * ENTITIES_PER_CONNECTION;

    let mut conn = db.begin_ro().await.unwrap();
    let t = Instant::now();
    let visible = conn
        .connection_catalogs()
        .visible(
            &CatalogFilter::default(),
            &VisibleProjects::All,
            &VisibleProjects::All,
        )
        .await?;
    let ids: Vec<ConnectionId> = visible.iter().map(|row| row.connection_id).collect();
    let named = conn
        .connection_catalogs()
        .of(&ids[..ids.len().min(30)], &VisibleProjects::All)
        .await?;
    let texts = conn.connection_catalogs().entity_texts(&ids[..1]).await?;
    let entity_ids: Vec<_> = texts.iter().map(|row| row.id).collect();
    let resolved = conn
        .connection_catalogs()
        .entities(&entity_ids, &VisibleProjects::All)
        .await?;
    record(
        "connection_catalog::visible",
        profile,
        t,
        visible.len(),
        connections,
    );
    assert!(visible.len() >= connections && visible.iter().all(|row| row.develop));
    assert_eq!(named.len(), ids.len().min(30));
    assert_eq!(resolved.len(), ENTITIES_PER_CONNECTION);
    drop(conn);

    let mut conn = db.begin().await.unwrap();
    let t = Instant::now();
    let changed = conn
        .connection_catalogs()
        .changed_after(&TextRevision::default(), 500)
        .await?;
    let batch: Vec<ConnectionId> = changed.iter().map(|row| row.connection_id).collect();
    let texts = conn.connection_catalogs().entity_texts(&batch).await?;
    let live = conn.connection_catalogs().live(None, 500).await?;
    let again = conn.connection_catalogs().texts_of(&batch).await?;
    assert_eq!(again.len(), batch.len());
    conn.connection_catalogs().text_changed(&batch[0]).await?;
    conn.connection_catalogs().texts_written(&batch[1]).await?;
    record(
        "connection_catalog::text_revisions",
        profile,
        t,
        changed.len() + texts.len() + live.len(),
        entities,
    );
    conn.commit().await.unwrap();
    assert_eq!(texts.len(), batch.len() * ENTITIES_PER_CONNECTION);
    let after = changed.last().unwrap().text_revision;
    let mut conn = db.begin_ro().await.unwrap();
    let bumped = conn.connection_catalogs().changed_after(&after, 10).await?;
    assert_eq!(
        bumped
            .iter()
            .map(|row| row.connection_id)
            .collect::<Vec<_>>(),
        [batch[0], batch[1]],
        "a text change moves the catalog past every earlier revision"
    );
    Ok(())
}

/// Times every catalog query, in a fixed order so two passes zip 1:1.
pub(crate) async fn time_catalog_queries(db: &TdPool) -> Result<Vec<(&'static str, f64)>, TdError> {
    let mut out = Vec::new();
    let mut conn = db.begin_ro().await.unwrap();
    let _ = timed!(
        out,
        "connection_catalog::visible",
        conn.connection_catalogs()
            .visible(
                &CatalogFilter::default(),
                &VisibleProjects::All,
                &VisibleProjects::All
            )
            .await?
    );
    let _ = timed!(
        out,
        "connection_catalog::text_revisions",
        conn.connection_catalogs()
            .changed_after(&TextRevision::default(), 500)
            .await?
    );
    Ok(out)
}

#[ignore = "expensive: seeds the catalog dataset at two sizes and times every catalog query"]
#[td_test::test(db)]
async fn report_scaling_connection_catalog(db: TdPool) -> Result<(), TdError> {
    const SMALL: usize = 40;
    const BIG: usize = 160;
    catalog_dataset(&db, SMALL).await?;
    analyze(&db, "after small catalog seed").await;
    let t1 = time_catalog_queries(&db).await?;
    catalog_dataset(&db, BIG).await?;
    analyze(&db, "after big catalog seed").await;
    let t2 = time_catalog_queries(&db).await?;

    let n_small = (SMALL * ENTITIES_PER_CONNECTION) as f64;
    let n_big = ((SMALL + BIG) * ENTITIES_PER_CONNECTION) as f64;
    assert_eq!(t1.len(), t2.len(), "two passes must time the same queries");
    for ((name, a), (name2, b)) in t1.iter().zip(&t2) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        assert_subquadratic(name, n_small, *a, n_big, *b);
    }
    Ok(())
}
