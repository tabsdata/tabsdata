//
// Copyright 2025 Tabs Data Inc.
//

pub mod collections;
pub mod datasets;
pub mod users;

use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, SqliteConnection};
use std::collections::HashMap;
use td_database::sql::create_bindings_literal;
use td_error::td_error;

/// Error type for the [`EntityFinder`] trait.
#[td_error]
pub enum EntityFinderError {
    #[error("Entity for ID not found: {0}")]
    IdNotFound(String) = 1000,
    #[error("Entity for Name not found: {0}")]
    NameNotFound(String) = 1001,
    #[error("Internal error: {0}")]
    SqlError(#[source] sqlx::Error) = 5000,
}

/// [`Result`] type for the [`EntityFinder`] trait.
pub type Result<T> = std::result::Result<T, EntityFinderError>;

/// Trait for entities that have an ID and a name.
pub trait IdName: Clone {
    /// Return the ID of the entity.
    fn id(&self) -> &str;

    /// Return the name of the entity.
    fn name(&self) -> &str;
}

/// Finds entities by ids or names.
pub struct EntityFinder<E>
where
    E: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    finder: EntityFinderImpl<E>,
}

impl<E> EntityFinder<E>
where
    E: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    /// Create a new instance of an entity finder.
    ///
    /// The `by_ids_sql_in_template` and `by_names_sql_in_template` are SQL templates that must
    /// do a SELECT query as follows:
    ///   * The SELECT columns must match the [`E`] fields (for [`FromRow`] to work)
    ///   * The WHERE clause must have an IN() statement with `{}` in it, this will be replaced
    ///     by the actual list of ?1, ?2, ... parameters based on the number of keys to find.
    ///   * The by_ids template must find entities by their IDs.
    ///   * The by_names template must find entities by their names.
    pub fn new<EE>(
        by_ids_sql_in_template: &'static str,
        by_names_sql_in_template: &'static str,
    ) -> EntityFinder<EE>
    where
        EE: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        EntityFinder {
            finder: EntityFinderImpl::<EE>::without_scope(
                by_ids_sql_in_template,
                by_names_sql_in_template,
            ),
        }
    }

    /// Find the name of an entity by its ID.
    pub async fn find_name(&self, conn: &mut SqliteConnection, id: &str) -> Result<String> {
        self.finder.find_name(conn, None, id).await
    }

    /// Find the ID of an entity by its name.
    pub async fn find_id(&self, conn: &mut SqliteConnection, name: &str) -> Result<String> {
        self.finder.find_id(conn, None, name).await
    }

    /// Find the entities IDs for the given names.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_ids(
        &self,
        conn: &mut SqliteConnection,
        names: &[&str],
    ) -> Result<Vec<Option<String>>> {
        self.finder.find_ids(conn, None, names).await
    }

    /// Find the entities ids for the given names returning an (NAME, ID) map.
    ///
    /// If an NAME does not exist it returns an [`EntityFinderError::NameNotFound`] error.
    pub async fn find_ids_as_map(
        &self,
        conn: &mut SqliteConnection,
        names: &[&str],
    ) -> Result<HashMap<String, String>> {
        self.finder.find_ids_as_map(conn, None, names).await
    }

    /// Find the entities names for the given IDs.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_names(
        &self,
        conn: &mut SqliteConnection,
        ids: &[&str],
    ) -> Result<Vec<Option<String>>> {
        self.finder.find_names(conn, None, ids).await
    }

    /// Find the entities names for the given IDs returning an (ID, NAME) map.
    ///
    /// If an ID does not exist it returns an [`EntityFinderError::IdNotFound`] error.
    pub async fn find_names_as_map(
        &self,
        conn: &mut SqliteConnection,
        ids: &[&str],
    ) -> Result<HashMap<String, String>> {
        self.finder.find_names_as_map(conn, None, ids).await
    }

    /// Find the entity for the given ID. Returns [`EntityFinderError::NotFound`] if not found.
    pub async fn find_by_id(&self, conn: &mut SqliteConnection, id: &str) -> Result<E> {
        self.finder.find_by_id(conn, None, id).await
    }

    /// Find the entity for the given name. Returns [`EntityFinderError::NotFound`] if not found.
    pub async fn find_by_name(&self, conn: &mut SqliteConnection, name: &str) -> Result<E> {
        self.finder.find_by_name(conn, None, name).await
    }

    /// Find the entities for the given IDs.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_by_ids(
        &self,
        conn: &mut SqliteConnection,
        ids: &[&str],
    ) -> Result<Vec<Option<E>>> {
        self.finder.find_by_ids(conn, None, ids).await
    }

    /// Find the entities for the given names.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_by_names(
        &self,
        conn: &mut SqliteConnection,
        names: &[&str],
    ) -> Result<Vec<Option<E>>> {
        self.finder.find_by_names(conn, None, names).await
    }
}

/// Finds entities by ids or names within the scope of another column value.
pub struct ScopedEntityFinder<E>
where
    E: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    finder: EntityFinderImpl<E>,
}

impl<E> ScopedEntityFinder<E>
where
    E: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    /// Create a new instance of a scoped entity finder.
    ///
    /// The `by_ids_sql_in_template` and `by_names_sql_in_template` are SQL templates that must
    /// do a SELECT query as follows:
    ///   * The SELECT columns must match the [`E`] fields (for [`FromRow`] to work)
    ///   * The WHERE clause must have an IN() statement with `{}` in it, this will be replaced
    ///     by the actual list of ?1, ?2, ... parameters based on the number of keys to find.
    ///   * The by_ids template must find entities by their IDs.
    ///   * The by_names template must find entities by their names.
    pub fn new<EE>(
        by_ids_sql_in_template: &'static str,
        by_names_sql_in_template: &'static str,
    ) -> ScopedEntityFinder<EE>
    where
        EE: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        ScopedEntityFinder {
            finder: EntityFinderImpl::<EE>::with_scope(
                by_ids_sql_in_template,
                by_names_sql_in_template,
            ),
        }
    }

    /// Find the name of an entity by its ID.
    pub async fn find_name(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        id: &str,
    ) -> Result<String> {
        self.finder.find_name(conn, Some(scope), id).await
    }

    /// Find the ID of an entity by its name.
    pub async fn find_id(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        name: &str,
    ) -> Result<String> {
        self.finder.find_id(conn, Some(scope), name).await
    }

    /// Find the entities IDs for the given names.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_ids(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        names: &[&str],
    ) -> Result<Vec<Option<String>>> {
        self.finder.find_ids(conn, Some(scope), names).await
    }

    /// Find the entities ids for the given names returning an (NAME, ID) map.
    ///
    /// If an NAME does not exist it returns an [`EntityFinderError::NameNotFound`] error.
    pub async fn find_ids_as_map(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        names: &[&str],
    ) -> Result<HashMap<String, String>> {
        self.finder.find_ids_as_map(conn, Some(scope), names).await
    }

    /// Find the entities names for the given IDs.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_names(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        ids: &[&str],
    ) -> Result<Vec<Option<String>>> {
        self.finder.find_names(conn, Some(scope), ids).await
    }

    /// Find the entities names for the given IDs returning an (ID, NAME) map.
    ///
    /// If an ID does not exist it returns an [`EntityFinderError::IdNotFound`] error.
    pub async fn find_names_as_map(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        ids: &[&str],
    ) -> Result<HashMap<String, String>> {
        self.finder.find_names_as_map(conn, Some(scope), ids).await
    }

    /// Find the entity for the given ID. Returns [`EntityFinderError::NotFound`] if not found.
    pub async fn find_by_id(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        id: &str,
    ) -> Result<E> {
        self.finder.find_by_id(conn, Some(scope), id).await
    }

    /// Find the entity for the given name. Returns [`EntityFinderError::NotFound`] if not found.
    pub async fn find_by_name(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        name: &str,
    ) -> Result<E> {
        self.finder.find_by_name(conn, Some(scope), name).await
    }

    /// Find the entities for the given IDs.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_by_ids(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        ids: &[&str],
    ) -> Result<Vec<Option<E>>> {
        self.finder.find_by_ids(conn, Some(scope), ids).await
    }

    /// Find the entities for the given names.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_by_names(
        &self,
        conn: &mut SqliteConnection,
        scope: &str,
        names: &[&str],
    ) -> Result<Vec<Option<E>>> {
        self.finder.find_by_names(conn, Some(scope), names).await
    }
}

static CHUNK_SIZE: usize = 200;

struct EntityFinderImpl<E>
where
    E: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    with_scope: bool,
    by_ids_sql_in_template: &'static str,
    by_names_sql_in_template: &'static str,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> EntityFinderImpl<E>
where
    E: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
{
    /// Create a new instance of the entity finder.
    ///
    /// The `by_ids_sql_in_template` and `by_names_sql_in_template` are SQL templates that must
    /// do a SELECT query as follows:
    ///   * The SELECT columns must match the [`E`] fields (for [`FromRow`] to work)
    ///   * The WHERE clause must have an IN() statement with `{}` in it, this will be replaced
    ///     by the actual list of ?1, ?2, ... parameters based on the number of keys to find.
    ///   * The by_ids template must find entities by their IDs.
    ///   * The by_names template must find entities by their names.
    pub fn with_scope<EE>(
        by_ids_sql_in_template: &'static str,
        by_names_sql_in_template: &'static str,
    ) -> EntityFinderImpl<EE>
    where
        EE: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        if !by_ids_sql_in_template.contains("?1") {
            panic!("Missing scope parameter in by_ids_sql_in_template");
        }
        if !by_names_sql_in_template.contains("?1") {
            panic!("Missing scope parameter in by_names_sql_in_template");
        }
        EntityFinderImpl {
            with_scope: true,
            by_ids_sql_in_template,
            by_names_sql_in_template,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new instance of the entity finder.
    ///
    /// The `by_ids_sql_in_template` and `by_names_sql_in_template` are SQL templates that must
    /// do a SELECT query as follows:
    ///   * The SELECT columns must match the [`E`] fields (for [`FromRow`] to work)
    ///   * The WHERE clause must have an IN() statement with `{}` in it, this will be replaced
    ///     by the actual list of ?1, ?2, ... parameters based on the number of keys to find.
    ///   * The by_ids template must find entities by their IDs.
    ///   * The by_names template must find entities by their names.
    pub fn without_scope<EE>(
        by_ids_sql_in_template: &'static str,
        by_names_sql_in_template: &'static str,
    ) -> EntityFinderImpl<EE>
    where
        EE: IdName + for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        if by_ids_sql_in_template.contains("?1") {
            panic!("It should not have scope parameter in by_ids_sql_in_template");
        }
        if by_names_sql_in_template.contains("?1") {
            panic!("It should not have scope parameter in by_names_sql_in_template");
        }
        EntityFinderImpl {
            with_scope: false,
            by_ids_sql_in_template,
            by_names_sql_in_template,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Find the name of an entity by its ID.
    pub async fn find_name(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        id: &str,
    ) -> Result<String> {
        self.find_by_id(conn, scope, id)
            .await
            .map(|e| e.name().to_string())
    }

    /// Find the ID of an entity by its name.
    pub async fn find_id(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        name: &str,
    ) -> Result<String> {
        self.find_by_name(conn, scope, name)
            .await
            .map(|e| e.id().to_string())
    }

    /// Convert a list of keys and values into a map of key-value pairs failing if a value is None.
    ///
    /// Used by the `find_ids_as_map` and `find_names_as_map` methods.
    fn convert_to_map(
        keys: &[&str],
        values: Vec<Option<String>>,
        err: fn(String) -> EntityFinderError,
    ) -> Result<HashMap<String, String>> {
        let mut map = HashMap::new();
        for (key, value) in keys.iter().zip(values.into_iter()) {
            match value {
                Some(v) => {
                    map.insert(key.to_string(), v);
                }
                None => return Err(err(key.to_string())),
            }
        }
        Ok(map)
    }

    /// Find the entities IDs for the given names.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_ids(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        names: &[&str],
    ) -> Result<Vec<Option<String>>> {
        Ok(self
            .find_by_names(conn, scope, names)
            .await?
            .into_iter()
            .map(|e| e.map(|e| e.id().to_string()))
            .collect())
    }

    /// Find the entities ids for the given names returning an (NAME, ID) map.
    ///
    /// If an NAME does not exist it returns an [`EntityFinderError::NameNotFound`] error.
    pub async fn find_ids_as_map(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        names: &[&str],
    ) -> Result<HashMap<String, String>> {
        let values = self.find_ids(conn, scope, names).await?;
        Self::convert_to_map(names, values, EntityFinderError::NameNotFound)
    }

    /// Find the entities names for the given IDs.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_names(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        ids: &[&str],
    ) -> Result<Vec<Option<String>>> {
        Ok(self
            .find_by_ids(conn, scope, ids)
            .await?
            .into_iter()
            .map(|e| e.map(|e| e.name().to_string()))
            .collect())
    }

    /// Find the entities names for the given IDs returning an (ID, NAME) map.
    ///
    /// If an ID does not exist it returns an [`EntityFinderError::IdNotFound`] error.
    pub async fn find_names_as_map(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        ids: &[&str],
    ) -> Result<HashMap<String, String>> {
        let values = self.find_names(conn, scope, ids).await?;
        Self::convert_to_map(ids, values, EntityFinderError::IdNotFound)
    }

    /// Find the entity for the given ID. Returns [`EntityFinderError::NotFound`] if not found.
    pub async fn find_by_id(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        id: &str,
    ) -> Result<E> {
        match self.find_by_ids(conn, scope, &[id]).await {
            Ok(mut v) if v[0].is_some() => Ok(v.pop().unwrap().unwrap()),
            Ok(_) => Err(EntityFinderError::IdNotFound(id.to_string())),
            Err(e) => Err(e),
        }
    }

    /// Find the entity for the given name. Returns [`EntityFinderError::NotFound`] if not found.
    pub async fn find_by_name(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        name: &str,
    ) -> Result<E> {
        match self.find_by_names(conn, scope, &[name]).await {
            Ok(mut v) if v[0].is_some() => Ok(v.pop().unwrap().unwrap()),
            Ok(_) => Err(EntityFinderError::NameNotFound(name.to_string())),
            Err(e) => Err(e),
        }
    }

    fn assert_scope_call(&self, scope: Option<&str>) -> Result<()> {
        if self.with_scope && scope.is_none() {
            return Err(EntityFinderError::SqlError(sqlx::Error::Protocol(
                "Scope is required".to_string(),
            )));
        }
        if !self.with_scope && scope.is_some() {
            return Err(EntityFinderError::SqlError(sqlx::Error::Protocol(
                "Scope is not allowed".to_string(),
            )));
        }
        Ok(())
    }

    /// Find the entities for the given IDs.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_by_ids(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        ids: &[&str],
    ) -> Result<Vec<Option<E>>> {
        self.assert_scope_call(scope)?;
        Self::find_by_keys(conn, self.by_ids_sql_in_template, scope, E::id, ids).await
    }

    /// Find the entities for the given names.
    /// For not found entities it returns None in the corresponding position.
    pub async fn find_by_names(
        &self,
        conn: &mut SqliteConnection,
        scope: Option<&str>,
        names: &[&str],
    ) -> Result<Vec<Option<E>>> {
        self.assert_scope_call(scope)?;
        Self::find_by_keys(conn, self.by_names_sql_in_template, scope, E::name, names).await
    }

    async fn find_by_keys_chunk(
        conn: &mut SqliteConnection,
        sql_in_template: &str,
        scope: Option<&str>,
        key_f: fn(&E) -> &str,
        key_chunk: &[&str],
    ) -> Result<Vec<Option<E>>>
    where
        E: IdName + for<'a> FromRow<'a, SqliteRow> + Send + Unpin,
    {
        let offset: usize = if scope.is_some() { 1 } else { 0 };
        let query =
            sql_in_template.replace("{}", &create_bindings_literal(offset, key_chunk.len()));

        let mut query_as = sqlx::query_as(&query);

        if let Some(scope) = scope {
            query_as = query_as.bind(scope);
        }

        for key in key_chunk.iter() {
            query_as = query_as.bind(key);
        }

        let map: HashMap<String, E> = query_as
            .fetch_all(conn)
            .await
            .map_err(EntityFinderError::SqlError)?
            .into_iter()
            .map(|e: E| (key_f(&e).to_string(), e))
            .collect();
        Ok(key_chunk.iter().map(|key| map.get(*key).cloned()).collect())
    }

    async fn find_by_keys(
        conn: &mut SqliteConnection,
        sql_in_template: &str,
        scope: Option<&str>,
        key_f: fn(&E) -> &str,
        keys: &[&str],
    ) -> Result<Vec<Option<E>>>
    where
        E: IdName + for<'a> FromRow<'a, SqliteRow> + Send + Unpin,
    {
        let mut names = Vec::new();

        for chunk in keys.chunks(CHUNK_SIZE) {
            let chunk_names =
                Self::find_by_keys_chunk(conn, sql_in_template, scope, key_f, chunk).await?;
            names.extend(chunk_names);
        }
        Ok(names)
    }
}

#[cfg(test)]
mod tests_without_scope {
    use crate::entity_finder::{EntityFinder, EntityFinderError, IdName, CHUNK_SIZE};
    use sqlx::FromRow;
    use td_database::sql::DbPool;

    #[derive(Debug, Clone, FromRow)]
    struct Foo {
        id: String,
        name: String,
    }

    impl IdName for Foo {
        fn id(&self) -> &str {
            &self.id
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    async fn create_test_data(db: &DbPool, rows: usize) {
        let mut trx = db.begin().await.unwrap();
        for i in 0..rows {
            sqlx::query("INSERT INTO foo (id, name) VALUES (?, ?)")
                .bind(i.to_string())
                .bind(format!("foo{}", i))
                .execute(&mut *trx)
                .await
                .unwrap();
        }
        trx.commit().await.unwrap();
    }

    fn create_test_finder() -> EntityFinder<Foo> {
        EntityFinder::<Foo>::new(
            "SELECT id, name FROM foo WHERE id IN ({})",
            "SELECT id, name FROM foo WHERE name IN ({})",
        )
    }

    #[tokio::test]
    async fn test_find_id() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        assert_eq!(finder.find_id(&mut conn, "foo0").await.unwrap(), "0");

        // Not found
        assert!(matches!(
            finder.find_id(&mut conn, "fooA").await,
            Err(EntityFinderError::NameNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_name() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        assert_eq!(finder.find_name(&mut conn, "0").await.unwrap(), "foo0");

        // Not found
        assert!(matches!(
            finder.find_name(&mut conn, "A").await,
            Err(EntityFinderError::IdNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_id() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder.find_by_id(&mut conn, "0").await.unwrap();
        assert_eq!(result.id, "0");
        assert_eq!(result.name, "foo0");

        // Not found
        assert!(matches!(
            finder.find_by_id(&mut conn, "A").await,
            Err(EntityFinderError::IdNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_name() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder.find_by_name(&mut conn, "foo0").await.unwrap();
        assert_eq!(result.id, "0");
        assert_eq!(result.name, "foo0");

        // Not found
        assert!(matches!(
            finder.find_by_name(&mut conn, "fooA").await,
            Err(EntityFinderError::NameNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_ids() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder.find_by_ids(&mut conn, &["0", "A"]).await.unwrap();
        assert_eq!(result.len(), 2);
        if let Some(foo) = &result[0] {
            assert_eq!(foo.id, "0");
            assert_eq!(foo.name, "foo0");
        } else {
            panic!()
        }
        assert!(result[1].is_none());
    }

    #[tokio::test]
    async fn test_find_by_names() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder
            .find_by_names(&mut conn, &["foo0", "foo0", "fooA", "fooA"])
            .await
            .unwrap();
        assert_eq!(result.len(), 4);
        if let Some(foo) = &result[0] {
            assert_eq!(foo.id, "0");
            assert_eq!(foo.name, "foo0");
        } else {
            panic!()
        }
        if let Some(foo) = &result[1] {
            assert_eq!(foo.id, "0");
            assert_eq!(foo.name, "foo0");
        } else {
            panic!()
        }
        assert!(result[2].is_none());
        assert!(result[3].is_none());
    }

    #[tokio::test]
    async fn test_chunking() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, CHUNK_SIZE * 2).await;
        let finder = create_test_finder();

        let mut keys_names: Vec<_> = (0..CHUNK_SIZE)
            .map(|i| (i.to_string(), Some(format!("foo{}", i))))
            .collect();
        keys_names.insert(0, ("A".to_string(), None));

        let result = finder
            .find_by_ids(
                &mut conn,
                keys_names
                    .iter()
                    .map(|kn| kn.0.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), CHUNK_SIZE + 1);
        assert!(result[0].as_ref().is_none());
        assert!(result[0].is_none());
        for i in 1..=CHUNK_SIZE {
            assert_eq!(result[i].as_ref().unwrap().id, keys_names[i].0);
            assert_eq!(
                result[i].as_ref().unwrap().name,
                keys_names[i].1.as_ref().unwrap().as_str()
            );
        }
    }

    #[tokio::test]
    async fn find_ids_as_map() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder
            .find_ids_as_map(&mut conn, &["foo0", "foo0"])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("foo0").unwrap(), "0");

        // Not found
        assert!(matches!(
            finder.find_ids_as_map(&mut conn, &["fooA"]).await,
            Err(EntityFinderError::NameNotFound(_))
        ));
    }

    #[tokio::test]
    async fn find_names_as_map() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder.find_names_as_map(&mut conn, &["0"]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("0").unwrap(), "foo0");

        // Not found
        assert!(matches!(
            finder.find_names_as_map(&mut conn, &["A"]).await,
            Err(EntityFinderError::IdNotFound(_))
        ));
    }
}

#[cfg(test)]
mod tests_with_scope {
    use crate::entity_finder::{EntityFinderError, IdName, ScopedEntityFinder, CHUNK_SIZE};
    use sqlx::FromRow;
    use td_database::sql::DbPool;

    #[derive(Debug, Clone, FromRow)]
    struct Foo {
        id: String,
        name: String,
    }

    impl IdName for Foo {
        fn id(&self) -> &str {
            &self.id
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    async fn create_test_data(db: &DbPool, rows: usize) {
        let mut trx = db.begin().await.unwrap();
        for i in 0..rows {
            sqlx::query("INSERT INTO foo_scoped (id, scope, name) VALUES (?, 'a', ?)")
                .bind(i.to_string())
                .bind(format!("foo{}", i))
                .execute(&mut *trx)
                .await
                .unwrap();
        }
        for i in 0..rows {
            sqlx::query("INSERT INTO foo_scoped (id, scope, name) VALUES (?, 'b', ?)")
                .bind(i.to_string())
                .bind(format!("foo{}", i))
                .execute(&mut *trx)
                .await
                .unwrap();
        }
        for i in 0..rows {
            sqlx::query("INSERT INTO foo_scoped (id, scope, name) VALUES (?, 'c', ?)")
                .bind(i.to_string())
                .bind(format!("bar{}", i))
                .execute(&mut *trx)
                .await
                .unwrap();
        }
        trx.commit().await.unwrap();
    }

    fn create_test_finder() -> ScopedEntityFinder<Foo> {
        ScopedEntityFinder::<Foo>::new(
            "SELECT id, name FROM foo_scoped WHERE scope = ?1 AND id IN ({})",
            "SELECT id, name FROM foo_scoped WHERE scope = ?1 AND name IN ({})",
        )
    }

    #[tokio::test]
    async fn test_find_id() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        assert_eq!(finder.find_id(&mut conn, "a", "foo0").await.unwrap(), "0");

        // Not found
        assert!(matches!(
            finder.find_id(&mut conn, "a", "fooA").await,
            Err(EntityFinderError::NameNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_name() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        assert_eq!(finder.find_name(&mut conn, "a", "0").await.unwrap(), "foo0");

        // Not found
        assert!(matches!(
            finder.find_name(&mut conn, "a", "A").await,
            Err(EntityFinderError::IdNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_id() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder.find_by_id(&mut conn, "a", "0").await.unwrap();
        assert_eq!(result.id, "0");
        assert_eq!(result.name, "foo0");

        // Not found
        assert!(matches!(
            finder.find_by_id(&mut conn, "a", "A").await,
            Err(EntityFinderError::IdNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_name() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder.find_by_name(&mut conn, "a", "foo0").await.unwrap();
        assert_eq!(result.id, "0");
        assert_eq!(result.name, "foo0");

        // Not found
        assert!(matches!(
            finder.find_by_name(&mut conn, "a", "fooA").await,
            Err(EntityFinderError::NameNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_ids() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder
            .find_by_ids(&mut conn, "a", &["0", "A"])
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        if let Some(foo) = &result[0] {
            assert_eq!(foo.id, "0");
            assert_eq!(foo.name, "foo0");
        } else {
            panic!()
        }
        assert!(result[1].is_none());
    }

    #[tokio::test]
    async fn test_find_by_names() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder
            .find_by_names(&mut conn, "a", &["foo0", "foo0", "fooA", "fooA"])
            .await
            .unwrap();
        assert_eq!(result.len(), 4);
        if let Some(foo) = &result[0] {
            assert_eq!(foo.id, "0");
            assert_eq!(foo.name, "foo0");
        } else {
            panic!();
        }
        if let Some(foo) = &result[1] {
            assert_eq!(foo.id, "0");
            assert_eq!(foo.name, "foo0");
        } else {
            panic!();
        }
        assert!(result[2].is_none());
        assert!(result[3].is_none());
    }

    #[tokio::test]
    async fn test_chunking() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, CHUNK_SIZE * 2).await;
        let finder = create_test_finder();

        let mut keys_names: Vec<_> = (0..CHUNK_SIZE)
            .map(|i| (i.to_string(), Some(format!("foo{}", i))))
            .collect();
        keys_names.insert(0, ("A".to_string(), None));

        let result = finder
            .find_by_ids(
                &mut conn,
                "a",
                keys_names
                    .iter()
                    .map(|kn| kn.0.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), CHUNK_SIZE + 1);
        assert!(result[0].as_ref().is_none());
        assert!(result[0].is_none());
        for i in 1..=CHUNK_SIZE {
            assert_eq!(result[i].as_ref().unwrap().id, keys_names[i].0);
            assert_eq!(
                result[i].as_ref().unwrap().name,
                keys_names[i].1.as_ref().unwrap().as_str()
            );
        }
    }

    #[tokio::test]
    async fn find_ids_as_map() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder
            .find_ids_as_map(&mut conn, "a", &["foo0", "foo0"])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("foo0").unwrap(), "0");

        // Not found
        assert!(matches!(
            finder.find_ids_as_map(&mut conn, "a", &["fooA"]).await,
            Err(EntityFinderError::NameNotFound(_))
        ));
    }

    #[tokio::test]
    async fn find_names_as_map() {
        let db = td_database::test_utils::test_db().await.unwrap();
        let mut conn = db.acquire().await.unwrap();
        create_test_data(&db, 2).await;
        let finder = create_test_finder();

        // Ok
        let result = finder
            .find_names_as_map(&mut conn, "a", &["0"])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("0").unwrap(), "foo0");

        // Not found
        assert!(matches!(
            finder.find_names_as_map(&mut conn, "a", &["A"]).await,
            Err(EntityFinderError::IdNotFound(_))
        ));
    }
}
