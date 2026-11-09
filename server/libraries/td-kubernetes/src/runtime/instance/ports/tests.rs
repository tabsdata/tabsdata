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
    assert_eq!(ports.len(), PORT_CATALOG.len() + AFTER_KMINUS.len());
    for role in PORT_CATALOG.iter().chain(AFTER_KMINUS) {
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
fn a_port_held_on_the_ipv6_wildcard_address_is_not_available() {
    let Ok(listener) = TcpListener::bind((std::net::Ipv6Addr::UNSPECIFIED, 0)) else {
        eprintln!("IPv6 is not available on this host, skipping");
        return;
    };
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

#[cfg(feature = "enterprise")]
#[test]
fn a_renamed_role_keeps_its_port() {
    let mut ports = vec![AssignedPort {
        role: "aiagent_public_port".to_owned(),
        caption: "aiagent public port".to_owned(),
        port: 3659,
    }];
    rename_roles(&mut ports);
    assert_eq!(ports, assigned(&[(PortRole::AiagentPrivate, 3659)]));
}

#[test]
fn a_current_role_is_left_alone() {
    let mut ports = assigned(&[(PortRole::ApiserverPublic, 2457)]);
    rename_roles(&mut ports);
    assert_eq!(ports, assigned(&[(PortRole::ApiserverPublic, 2457)]));
}

#[cfg(feature = "enterprise")]
#[test]
fn the_runner_and_embedder_ports_follow_the_ports_kminus_binds() {
    let ports = assign_ports(DEFAULT_BASE_PORT);
    let port_of = |role: PortRole| {
        ports
            .iter()
            .find(|entry| entry.role == role.as_str())
            .map(|entry| entry.port)
    };
    assert_eq!(port_of(PortRole::KminusApi), Some(DEFAULT_BASE_PORT + 4));
    assert_eq!(
        port_of(PortRole::AitdkPrivate),
        Some(DEFAULT_BASE_PORT + 4 + KMINUS_RESERVED_PORTS)
    );
    assert_eq!(
        port_of(PortRole::EmbedderPrivate),
        Some(DEFAULT_BASE_PORT + 5 + KMINUS_RESERVED_PORTS)
    );
}

#[cfg(feature = "enterprise")]
#[test]
fn a_stored_instance_gains_the_roles_added_since() {
    let mut ports = assigned(&[
        (PortRole::ApiserverPublic, 3657),
        (PortRole::ApiserverPrivate, 3658),
        (PortRole::AiagentPrivate, 3659),
        (PortRole::Database, 3660),
        (PortRole::KminusApi, 3661),
    ]);
    add_missing_roles(&mut ports);
    assert_eq!(ports, assign_ports(3657));
}

#[test]
fn a_complete_instance_is_left_alone() {
    let mut ports = assign_ports(3657);
    ports[1].port = 4000;
    let before = ports.clone();
    add_missing_roles(&mut ports);
    assert_eq!(ports, before);
}

#[test]
fn an_instance_without_an_apiserver_port_gains_nothing() {
    let mut ports = Vec::new();
    add_missing_roles(&mut ports);
    assert!(ports.is_empty());
}

#[test]
fn every_assigned_role_reads_back() {
    for entry in assign_ports(DEFAULT_BASE_PORT) {
        assert!(PortRole::from_str(&entry.role).is_some(), "{}", entry.role);
    }
}

fn table_field(port: u16) -> u32 {
    u32::from(port.to_be()) | 0xabcd_0000
}

#[test]
fn a_table_port_is_read_in_network_byte_order() {
    assert_eq!(table_port(table_field(2457)), 2457);
    assert_eq!(table_port(table_field(65535)), 65535);
}

#[test]
fn only_the_holders_of_the_port_are_returned_once_each() {
    let rows = [
        (table_field(2457), 100),
        (table_field(2458), 200),
        (table_field(2457), 100),
        (table_field(2457), 0),
        (table_field(2457), 300),
    ];
    assert_eq!(holders_of(rows, 2457), [100, 300]);
    assert!(holders_of(rows, 9999).is_empty());
}
