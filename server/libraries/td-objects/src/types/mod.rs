//
// Copyright 2025 Tabs Data Inc.
//

// Basic types reexported for convenience under basic module
pub mod basic;

pub mod addresses;
pub mod composed;
pub mod graph;
pub mod option;
pub mod recursion;
pub mod status_count;
pub mod visible_collections;

#[cfg(test)]
mod tests;

/// A trait for types that can be used as SQL entities.
pub trait SqlEntity: std::fmt::Debug + Send + Sync {
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>);

    fn push_bind_unseparated<'a>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
    );

    fn as_display(&self) -> String;
    fn from_display(s: impl ToString) -> Result<Self, td_error::TdError>
    where
        Self: Sized;

    fn as_dyn(&self) -> &dyn SqlEntity
    where
        Self: Sized,
    {
        self as &dyn SqlEntity
    }

    fn as_any(&self) -> &dyn std::any::Any;
    fn type_id(&self) -> std::any::TypeId {
        self.as_any().type_id()
    }
}

impl<T> SqlEntity for &T
where
    T: SqlEntity,
{
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>) {
        (**self).push_bind(builder)
    }

    fn push_bind_unseparated<'a>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
    ) {
        (**self).push_bind_unseparated(builder)
    }

    fn as_display(&self) -> String {
        (**self).as_display()
    }

    fn from_display(_s: impl ToString) -> Result<Self, td_error::TdError>
    where
        Self: Sized,
    {
        unreachable!("Cannot create reference from display string")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        (**self).as_any()
    }
}

pub trait AsDynSqlEntities: Send + Sync {
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity>;
}

// Slice of references
impl AsDynSqlEntities for [&dyn SqlEntity] {
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.to_vec()
    }
}

impl<const N: usize> AsDynSqlEntities for [&dyn SqlEntity; N] {
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.to_vec()
    }
}

impl<T> AsDynSqlEntities for [T]
where
    T: AsDynSqlEntities,
{
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.iter()
            .flat_map(AsDynSqlEntities::as_dyn_entities)
            .collect()
    }
}

impl<T, const N: usize> AsDynSqlEntities for [T; N]
where
    T: AsDynSqlEntities,
{
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.iter()
            .flat_map(AsDynSqlEntities::as_dyn_entities)
            .collect()
    }
}

impl<T> AsDynSqlEntities for Vec<T>
where
    T: AsDynSqlEntities,
{
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.iter()
            .flat_map(AsDynSqlEntities::as_dyn_entities)
            .collect()
    }
}

macro_rules! impl_dyn_tuples {
    (
        [$($T:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        impl<$($T: SqlEntity),*> AsDynSqlEntities for ($($T),*) {
            fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
                let ($($T),*) = self;
                vec![$($T.as_dyn()),*]
            }
        }
    };
}

all_the_tuples!(impl_dyn_tuples);

pub trait ComposedString {
    fn parse(s: impl Into<String>) -> Result<Self, td_error::TdError>
    where
        Self: Sized;

    fn compose(&self) -> String;
}
