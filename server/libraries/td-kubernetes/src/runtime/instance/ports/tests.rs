//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn assigned(entries: &[(&str, u16)]) -> Vec<AssignedPort> {
    entries
        .iter()
        .map(|(role, port)| AssignedPort {
            role: (*role).to_owned(),
            caption: (*role).to_owned(),
            port: *port,
        })
        .collect()
}

#[test]
fn every_port_selects_all_roles() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    let ports = assigned(&[("apiserver_public_port", 0), ("database_port", taken)]);
    let found = first_port_in_use(&ports, EVERY_PORT, NO_PATIENCE);
    assert_eq!(found.map(|entry| entry.port), Some(taken));
}

#[test]
fn host_published_ports_ignores_the_other_roles() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    let ports = assigned(&[("database_port", taken)]);
    assert!(first_port_in_use(&ports, HOST_PUBLISHED_PORTS, NO_PATIENCE).is_none());
}

#[test]
fn a_free_published_port_reports_nothing() {
    let ports = assigned(&[("apiserver_public_port", 0)]);
    assert!(first_port_in_use(&ports, HOST_PUBLISHED_PORTS, NO_PATIENCE).is_none());
}

#[test]
fn a_taken_published_port_is_reported() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    let ports = assigned(&[("apiserver_public_port", taken)]);
    let found = first_port_in_use(&ports, HOST_PUBLISHED_PORTS, NO_PATIENCE);
    assert_eq!(found.map(|entry| entry.port), Some(taken));
}
