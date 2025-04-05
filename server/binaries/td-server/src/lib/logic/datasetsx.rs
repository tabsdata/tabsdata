// //
// // Copyright 2024 Tabs Data Inc.
// //
//
// use chrono::NaiveDateTime;
// use getset::Getters;
//
// #[derive(Debug, Getters, sqlx::FromRow)]
// #[getset(get = "pub")]
// struct DataDependencies {
//     source: Option<String>,
//     target: String,
//     resolved_on: NaiveDateTime,
// }
//
// impl DataDependencies {
//     pub fn from<'a>(
//         tuples: impl Into<Vec<(Option<&'a str>, &'a str, NaiveDateTime)>>,
//     ) -> Vec<Self> {
//         tuples
//             .into()
//             .iter()
//             .map(|(source, target, resolved_on)| DataDependencies {
//                 source: source.map(|s| s.to_string()),
//                 target: target.to_string(),
//                 resolved_on: *resolved_on,
//             })
//             .collect()
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use td_objects::crudl::handle_create_error;
//     use td_database::sql::SqliteConfigBuilder;
//     use td_common::time::UniqueUtc;
//     use crate::logic::datasetsx::DataDependencies;
//     use td_database::test::db;
//     use getset::Getters;
//     use sqlx::SqliteConnection;
//
//     async fn insert_deps(conn: &mut SqliteConnection, data: &Vec<DataDependencies>) {
//         let sql = r#"
//             INSERT INTO data_dependencies (
//                 source,
//                 target,
//                 resolved_on
//             )
//             VALUES (?, ?, ?)
//         "#;
//
//         for data in data.iter() {
//             sqlx::query(sql)
//                 .bind(data.source())
//                 .bind(data.target())
//                 .bind(data.resolved_on())
//                 .execute(&mut *conn)
//                 .await
//                 .map_err(handle_create_error)
//                 .unwrap();
//         }
//     }
//
//     #[tokio::test]
//     async fn test_dependency_recursive_query_for_pristine_versions() {
//         let config = SqliteConfigBuilder::default()
//             .location("sqlite::memory:")
//             .build()
//             .unwrap();
//         let db = db(&config).await.unwrap();
//
//         let mut conn = db.begin().await.unwrap();
//
//         let t0 = UniqueUtc::now_millis();
//         let t1 = UniqueUtc::now_millis();
//         let t2 = UniqueUtc::now_millis();
//
//         insert_deps(
//             &mut conn,
//             &DataDependencies::from([
//                 (None, "A1", t0),
//                 (Some("A1"), "B1", t0),
//                 (Some("B1"), "C1", t0),
//                 (Some("A1"), "C1", t0),
//                 (Some("D0"), "B1", t0),
//                 (None, "D1", t1),
//                 (Some("D1"), "B2", t1),
//                 (Some("B2"), "C2", t1),
//                 (Some("A1"), "C2", t1),
//                 (None, "A2", t2),
//                 (Some("D1"), "B3", t2),
//                 (Some("A2"), "B3", t2),
//                 (Some("B3"), "C3", t2),
//                 (Some("A2"), "C3", t2),
//             ]),
//         )
//         .await;
//
//         conn.commit().await.unwrap();
//
//         let sql = r#"
//             WITH RECURSIVE deps(s, t, r) AS (
//                SELECT d.source, d.target, d.resolved_on FROM data_dependencies d WHERE d.target = ?
//                UNION ALL
//                SELECT d.source, d.target, d.resolved_on FROM data_dependencies d JOIN deps ON d.target = s
//             )
//             SELECT s as source, t as target, r as resolved_on FROM deps order by r
//         "#;
//
//         let res: Vec<DataDependencies> = sqlx::query_as(sql)
//             .bind("C3")
//             .fetch_all(&db)
//             .await
//             .expect("Failed to fetch data");
//         res.iter().for_each(|d| println!("{:?}", d));
//     }
//
//     #[derive(Debug, Getters, sqlx::FromRow)]
//     #[getset(get = "pub")]
//     struct Table {
//         id: String,
//         collection_id: String,
//         dataset_id: String,
//         function_id: String,
//         data_version: String,
//         table_name: String,
//         partition: Option<String>,
//         partition_deleted: Option<bool>,
//         schema_id: String,
//         data_location: String,
//         system_table: bool,
//     }
//
//     impl Table {
//         pub fn from<'a>(
//             tuples: impl Into<
//                 Vec<(
//                     &'a str,
//                     &'a str,
//                     &'a str,
//                     &'a str,
//                     &'a str,
//                     &'a str,
//                     Option<&'a str>,
//                     Option<bool>,
//                     &'a str,
//                     &'a str,
//                     bool,
//                 )>,
//             >,
//         ) -> Vec<Self> {
//             tuples
//                 .into()
//                 .iter()
//                 .map(
//                     |(
//                         id,
//                         collection_id,
//                         dataset_id,
//                         function_id,
//                         data_version,
//                         table_name,
//                         partition,
//                         partition_deleted,
//                         schema_id,
//                         data_location,
//                         system_table,
//                     )| Table {
//                         id: id.to_string(),
//                         collection_id: collection_id.to_string(),
//                         dataset_id: dataset_id.to_string(),
//                         function_id: function_id.to_string(),
//                         data_version: data_version.to_string(),
//                         table_name: table_name.to_string(),
//                         partition: partition.map(|s| s.to_string()),
//                         partition_deleted: *partition_deleted,
//                         schema_id: schema_id.to_string(),
//                         data_location: data_location.to_string(),
//                         system_table: *system_table,
//                     },
//                 )
//                 .collect()
//         }
//     }
//
//     async fn insert_tables(conn: &mut SqliteConnection, data: &Vec<Table>) {
//         let sql = r#"
//             INSERT INTO data_tables (
//                 id,
//                 collection_id,
//                 dataset_id,
//                 function_id,
//                 data_version,
//                 table_name,
//                 partition,
//                 partition_deleted,
//                 schema_id,
//                 data_location,
//                 system_table
//             )
//             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
//         "#;
//
//         for data in data.iter() {
//             sqlx::query(sql)
//                 .bind(data.id())
//                 .bind(data.collection_id())
//                 .bind(data.dataset_id())
//                 .bind(data.function_id())
//                 .bind(data.data_version())
//                 .bind(data.table_name())
//                 .bind(data.partition())
//                 .bind(data.partition_deleted())
//                 .bind(data.schema_id())
//                 .bind(data.data_location())
//                 .bind(data.system_table())
//                 .execute(&mut *conn)
//                 .await
//                 .map_err(handle_create_error)
//                 .unwrap();
//         }
//     }
//
//     #[tokio::test]
//     async fn test_table_finding() {
//         let config = SqliteConfigBuilder::default()
//             .location("sqlite::memory:")
//             .build()
//             .unwrap();
//         let db = db(&config).await.unwrap();
//
//         let mut conn = db.begin().await.unwrap();
//
//         insert_tables(
//             &mut conn,
//             &Table::from([
//                 (
//                     "1",
//                     "D1",
//                     "d1",
//                     "f1",
//                     "v0",
//                     "t1",
//                     Some("p0"),
//                     Some(false),
//                     "s1",
//                     "l1",
//                     false,
//                 ),
//                 (
//                     "2",
//                     "D1",
//                     "d1",
//                     "f1",
//                     "v2",
//                     "t1",
//                     Some("p1"),
//                     Some(false),
//                     "s1",
//                     "l2",
//                     false,
//                 ),
//                 (
//                     "3",
//                     "D1",
//                     "d1",
//                     "f1",
//                     "v4",
//                     "t1",
//                     Some("p0"),
//                     Some(true),
//                     "s2",
//                     "l3",
//                     false,
//                 ),
//             ]),
//         )
//         .await;
//
//         conn.commit().await.unwrap();
//
//         let sql = r#"
//             SELECT * from data_tables
//             INNER JOIN (
//                 SELECT table_name, partition, MAX(data_version) as max_version
//                 FROM data_tables
//                 WHERE collection_id = ? AND dataset_id = ? AND table_name = ? AND data_version <= ?
//                 GROUP BY table_name, partition
//             ) latest
//             ON data_tables.table_name = latest.table_name
//               AND data_tables.partition = latest.partition
//               AND data_tables.data_version = latest.max_version
//               AND data_tables.partition_deleted = false
//         "#;
//
//         let res: Vec<Table> = sqlx::query_as(sql)
//             .bind("D1")
//             .bind("d1")
//             .bind("t1")
//             .bind("v3")
//             .fetch_all(&db)
//             .await
//             .expect("Failed to fetch data");
//         res.iter().for_each(|d| println!("{:?}", d));
//     }
// }
