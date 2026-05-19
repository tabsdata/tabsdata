//
// Copyright 2025 Tabs Data Inc.
//

// The sqlx round-trip test below uses sqlite (its in-memory store is the
// most lightweight way to exercise Encode/Decode impls inline). It only
// compiles when the sqlite backend is on; the same `tm_objects::typed`
// types are independently round-tripped through postgres via the
// td-database test matrix, so this is overlap coverage, not the only path.
#[cfg(all(test, sqlite))]
mod tests {
    use crate as ta_objects;

    use sqlx::SqliteConnection;

    #[tm_objects::typed(string)]
    pub struct MyString;

    #[tm_objects::typed(i16)]
    pub struct MyI16;

    #[tm_objects::typed(i32)]
    pub struct MyI32;

    #[tm_objects::typed(i64)]
    pub struct MyI64;

    #[tm_objects::typed(f32)]
    pub struct Myf32;

    #[tm_objects::typed(f64)]
    pub struct Myf64;

    #[tm_objects::typed(bool)]
    pub struct MyBool;

    #[tm_objects::typed(timestamp)]
    pub struct MyTimestamp;

    #[derive(sqlx::FromRow)]
    struct MyStruct {
        id: i64,
        my_string: MyString,
        my_i16: MyI16,
        my_i32: MyI32,
        my_i64: MyI64,
        my_f32: Myf32,
        my_f64: Myf64,
        my_bool: MyBool,
        my_timestamp: MyTimestamp,
    }

    #[tokio::test]
    async fn test_sqlx_typed_bind_encode_decoded() {
        let my_string = MyString::try_from("hello").unwrap();
        let my_i16 = MyI16::try_from(16).unwrap();
        let my_i32 = MyI32::try_from(32).unwrap();
        let my_i64 = MyI64::try_from(64).unwrap();
        let my_f32 = Myf32::try_from(32.0).unwrap();
        let my_f64 = Myf64::try_from(64.0).unwrap();
        let my_bool = MyBool::from(true);
        let my_timestamp = MyTimestamp::now();

        let db = sqlx::sqlite::SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let mut conn = db.acquire().await.unwrap();
        let conn = &mut conn as &mut SqliteConnection;
        sqlx::query(
            r#"
            CREATE TABLE my_table (
                id INTEGER PRIMARY KEY,
                my_string TEXT,
                my_i16 INTEGER,
                my_i32 INTEGER,
                my_i64 INTEGER,
                my_f32 REAL,
                my_f64 REAL,
                my_bool BOOLEAN,
                my_timestamp TEXT
            )
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO my_table VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")
            .bind(&my_string)
            .bind(my_i16)
            .bind(my_i32)
            .bind(my_i64)
            .bind(my_f32)
            .bind(my_f64)
            .bind(my_bool)
            .bind(my_timestamp)
            .execute(&mut *conn)
            .await
            .unwrap();

        let got: MyStruct = sqlx::query_as("SELECT * FROM my_table")
            .fetch_one(&mut *conn)
            .await
            .unwrap();

        assert_eq!(got.id, 0);
        assert_eq!(*got.my_string, *my_string);
        assert_eq!(*got.my_i16, *my_i16);
        assert_eq!(*got.my_i32, *my_i32);
        assert_eq!(*got.my_i64, *my_i64);
        assert_eq!(*got.my_f32, *my_f32);
        assert_eq!(*got.my_f64, *my_f64);
        assert_eq!(*got.my_bool, *my_bool);
        assert_eq!(*got.my_timestamp, *my_timestamp);
    }
}
