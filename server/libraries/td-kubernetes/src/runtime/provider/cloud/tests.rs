//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn amazon_and_azure_are_verified_against_their_own_authority() {
    assert_eq!(
        database_ssl_root_cert_for(ClusterProvider::Eks),
        "/tabsdata/certs/ca-aws.pem"
    );
    assert_eq!(
        database_ssl_root_cert_for(ClusterProvider::Fargate),
        "/tabsdata/certs/ca-aws.pem"
    );
    assert_eq!(
        database_ssl_root_cert_for(ClusterProvider::Aks),
        "/tabsdata/certs/ca-azure.pem"
    );
}

#[test]
fn no_cloud_borrows_the_authority_of_another() {
    assert_ne!(
        database_ssl_root_cert_for(ClusterProvider::Aks),
        database_ssl_root_cert_for(ClusterProvider::Eks)
    );
}

#[test]
fn google_names_no_certificate_file() {
    assert_eq!(database_ssl_root_cert_for(ClusterProvider::Gke), YAML_NULL);
}

#[test]
fn a_local_database_needs_no_authority() {
    assert_eq!(
        database_ssl_root_cert_for(ClusterProvider::Kminus),
        YAML_NULL
    );
    assert_eq!(
        database_ssl_root_cert_for(ClusterProvider::Docker),
        YAML_NULL
    );
}
