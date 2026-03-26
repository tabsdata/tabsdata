//
// Copyright 2026 Tabs Data Inc.
//

use crate::dynamic::{DynamicConnection, DynamicPool};
use crate::pool::TdPool;

#[derive(Debug)]
pub enum DatabaseBackend {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
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
            #[cfg(feature = "sqlite")]
            DynamicPool::Sqlite { .. } => DatabaseBackend::Sqlite,
            #[cfg(feature = "postgres")]
            DynamicPool::Postgres { .. } => DatabaseBackend::Postgres,
            #[cfg(feature = "mysql")]
            DynamicPool::MySql { .. } => DatabaseBackend::MySql,
        }
    }
}

impl DatabaseKind for DynamicConnection<'_> {
    fn kind(&self) -> DatabaseBackend {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicConnection::Sqlite { .. } => DatabaseBackend::Sqlite,
            #[cfg(feature = "postgres")]
            DynamicConnection::Postgres { .. } => DatabaseBackend::Postgres,
            #[cfg(feature = "mysql")]
            DynamicConnection::MySql { .. } => DatabaseBackend::MySql,
        }
    }
}
