//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::HashMap;

use http::Method;
use td_common::worker::{Decorator, HttpRequest, WorkerName};
use td_objects::types::basic::{
    CollectionId, CollectionName, ConnectionKind, FunctionId, FunctionName, ProjectId, ProjectName,
    WorkerId,
};
use url::Url;

use super::*;

fn callback() -> Callback {
    Callback::Http(HttpRequest {
        url: Url::parse("http://localhost/callback").unwrap(),
        method: Method::GET,
        headers: HashMap::new(),
        body: false,
    })
}

#[test]
fn a_fully_qualified_conn_class_keeps_only_the_class() {
    assert_eq!(
        conn_class("tabsdatak.conn.mysql:MySQLSrcConn"),
        "MySQLSrcConn"
    );
    assert_eq!(
        conn_class("tabsdatak.conn.postgres:PostgresCdcStreamSrcConn"),
        "PostgresCdcStreamSrcConn"
    );
    assert_eq!(conn_class("tabsdatak.conn.s3:S3DestConn"), "S3DestConn");
}

#[test]
fn a_bare_class_is_left_alone() {
    assert_eq!(conn_class("MySQLSrcConn"), "MySQLSrcConn");
}

#[test]
fn only_the_last_colon_splits() {
    assert_eq!(conn_class("a:b:ClassName"), "ClassName");
}

#[test]
fn the_split_class_survives_label_sanitising() {
    let value = label_value(&conn_class("tabsdatak.conn.mysql:MySQLSrcConn"));
    assert_eq!(value, "MySQLSrcConn");
}

#[test]
fn connection_keeps_the_stored_value_and_connection_class_splits_it() {
    let request = FunctionStartRequest {
        worker: WorkerName::Function,
        worker_id: WorkerId::try_from("00000000000000000000000000").unwrap(),
        decorator: Decorator::Publisher,
        resolver: callback(),
        responder: callback(),
        project_id: ProjectId::default(),
        project: Some(ProjectName::try_from("project").unwrap()),
        collection_id: CollectionId::default(),
        collection: CollectionName::try_from("collection").unwrap(),
        connection_id: None,
        connection: Some(ConnectionKind::try_from("tabsdatak.conn.mysql:MySQLSrcConn").unwrap()),
        function_id: FunctionId::default(),
        function: FunctionName::try_from("function").unwrap(),
    };
    assert_eq!(
        request.connection(),
        Some("tabsdatak.conn.mysql:MySQLSrcConn".to_owned())
    );
    assert_eq!(request.connection_class(), Some("MySQLSrcConn".to_owned()));
}
