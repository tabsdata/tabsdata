//
// Copyright 2026 Tabsdata Inc.
//

use crate::dynamic::{DynamicConnection, DynamicPool};
use crate::pool::TdPool;
use td_schema::schema::backend::Backend;

#[derive(Debug)]
pub enum DatabaseBackend {
    #[cfg(sqlite)]
    Sqlite,
    #[cfg(postgres)]
    Postgres,
    #[cfg(mysql)]
    MySql,
}

impl From<DatabaseBackend> for Backend {
    fn from(backend: DatabaseBackend) -> Self {
        match backend {
            #[cfg(sqlite)]
            DatabaseBackend::Sqlite => Backend::Sqlite,
            #[cfg(postgres)]
            DatabaseBackend::Postgres => Backend::Postgres,
            #[cfg(mysql)]
            DatabaseBackend::MySql => Backend::MySql,
        }
    }
}

pub trait DatabaseKind {
    fn kind(&self) -> DatabaseBackend;
}

impl<T: DatabaseKind + ?Sized> DatabaseKind for &T {
    fn kind(&self) -> DatabaseBackend {
        (**self).kind()
    }
}

impl<T: DatabaseKind + ?Sized> DatabaseKind for &mut T {
    fn kind(&self) -> DatabaseBackend {
        (**self).kind()
    }
}

impl DatabaseKind for TdPool {
    fn kind(&self) -> DatabaseBackend {
        self.pool.kind()
    }
}

impl DatabaseKind for DynamicPool {
    fn kind(&self) -> DatabaseBackend {
        match self {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { .. } => DatabaseBackend::Sqlite,
            #[cfg(postgres)]
            DynamicPool::Postgres { .. } => DatabaseBackend::Postgres,
            #[cfg(mysql)]
            DynamicPool::MySql { .. } => DatabaseBackend::MySql,
        }
    }
}

impl DatabaseKind for DynamicConnection<'_> {
    fn kind(&self) -> DatabaseBackend {
        match self {
            #[cfg(sqlite)]
            DynamicConnection::Sqlite { .. } => DatabaseBackend::Sqlite,
            #[cfg(postgres)]
            DynamicConnection::Postgres { .. } => DatabaseBackend::Postgres,
            #[cfg(mysql)]
            DynamicConnection::MySql { .. } => DatabaseBackend::MySql,
        }
    }
}
