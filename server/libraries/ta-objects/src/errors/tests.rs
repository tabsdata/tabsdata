//
// Copyright 2026 Tabsdata Inc.
//

use super::{SqlError, classify};
use sqlx::error::{DatabaseError, ErrorKind};
use std::borrow::Cow;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

#[derive(Debug)]
struct Coded(&'static str);

impl Display for Coded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "sqlstate {}", self.0)
    }
}

impl Error for Coded {}

impl DatabaseError for Coded {
    fn message(&self) -> &str {
        self.0
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.0))
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}

#[derive(Clone, Default)]
struct Levels(Arc<Mutex<Vec<Level>>>);

impl Subscriber for Levels {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        self.0.lock().unwrap().push(*event.metadata().level());
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

fn logged(error: sqlx::Error) -> Vec<Level> {
    let levels = Levels::default();
    tracing::subscriber::with_default(levels.clone(), || {
        classify("entity".to_string(), error, &[]);
    });
    levels.0.lock().unwrap().clone()
}

fn database(code: &'static str) -> sqlx::Error {
    sqlx::Error::Database(Box::new(Coded(code)))
}

#[test]
fn conflicts_log_at_debug() {
    for code in ["40001", "40P01"] {
        assert_eq!(logged(database(code)), vec![Level::DEBUG], "{code}");
    }
}

#[test]
fn timeouts_log_at_warn() {
    for error in [
        database("55P03"),
        database("57014"),
        sqlx::Error::PoolTimedOut,
    ] {
        assert_eq!(logged(error), vec![Level::WARN]);
    }
}

#[test]
fn shortages_are_busy_and_log_at_warn() {
    assert_eq!(logged(database("53200")), vec![Level::WARN]);
    assert!(matches!(
        classify("entity".to_string(), database("53200"), &[]),
        SqlError::Busy(..)
    ));
}
