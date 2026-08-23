//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use crate::runtime::provider::ClusterProvider;

fn assigned(entries: &[(PortRole, u16)]) -> Vec<AssignedPort> {
    entries
        .iter()
        .map(|(role, port)| AssignedPort {
            role: role.as_str().to_owned(),
            caption: role.caption().to_owned(),
            port: *port,
        })
        .collect()
}

#[test]
fn every_catalog_role_round_trips_through_its_name() {
    for role in PORT_CATALOG {
        assert_eq!(PortRole::from_str(role.as_str()), Some(*role));
    }
}

#[test]
fn an_unknown_role_name_resolves_to_nothing() {
    assert_eq!(PortRole::from_str("not_a_port_role"), None);
}

#[test]
fn assigned_ports_cover_the_whole_catalog() {
    let ports = assign_ports(DEFAULT_BASE_PORT);
    assert_eq!(ports.len(), PORT_CATALOG.len());
    for role in PORT_CATALOG {
        assert!(ports.iter().any(|entry| entry.role == role.as_str()));
    }
}

#[test]
fn a_port_held_on_the_loopback_address_is_not_available() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    assert!(!port_available(taken));
}

#[test]
fn a_port_held_on_the_wildcard_address_is_not_available() {
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    assert!(!port_available(taken));
}

#[test]
fn a_released_port_becomes_available() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let released = listener.local_addr().unwrap().port();
    drop(listener);
    assert!(port_available(released));
}

#[test]
fn every_port_selects_all_roles() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    let ports = assigned(&[(PortRole::ApiserverPublic, 0), (PortRole::Database, taken)]);
    let found = first_port_in_use(&ports, CHECK_ALL_PORTS_FILTER, NO_PATIENCE);
    assert_eq!(found.map(|entry| entry.port), Some(taken));
}

#[test]
fn host_published_ports_ignores_the_other_roles() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    let ports = assigned(&[(PortRole::Database, taken)]);
    assert!(first_port_in_use(&ports, CHECK_ONLY_PUBLISHED_PORTS_FILTER, NO_PATIENCE).is_none());
}

#[test]
fn a_free_published_port_reports_nothing() {
    let ports = assigned(&[(PortRole::ApiserverPublic, 0)]);
    assert!(first_port_in_use(&ports, CHECK_ONLY_PUBLISHED_PORTS_FILTER, NO_PATIENCE).is_none());
}

#[test]
fn a_taken_published_port_is_reported() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let taken = listener.local_addr().unwrap().port();
    let ports = assigned(&[(PortRole::ApiserverPublic, taken)]);
    let found = first_port_in_use(&ports, CHECK_ONLY_PUBLISHED_PORTS_FILTER, NO_PATIENCE);
    assert_eq!(found.map(|entry| entry.port), Some(taken));
}

#[test]
fn every_provider_reports_the_ports_it_serves_on_the_host() {
    let ports = assign_ports(DEFAULT_BASE_PORT);
    for provider in [
        ClusterProvider::Docker,
        ClusterProvider::Eks,
        ClusterProvider::Fargate,
    ] {
        let roles: Vec<&str> = ports_matching(&ports, provider.ports_to_report())
            .iter()
            .map(|entry| entry.role.as_str())
            .collect();
        assert!(
            !roles.contains(&PortRole::Database.as_str()),
            "{provider:?} reports the database port"
        );
        assert!(
            !roles.contains(&PortRole::KminusApi.as_str()),
            "{provider:?} reports the kminus api port"
        );
        assert!(
            roles.contains(&PortRole::ApiserverPublic.as_str()),
            "{provider:?}"
        );
    }
}

#[test]
fn kminus_reports_every_assigned_port() {
    let ports = assign_ports(DEFAULT_BASE_PORT);
    let reported = ports_matching(&ports, ClusterProvider::Kminus.ports_to_report());
    assert_eq!(reported.len(), ports.len());
}
