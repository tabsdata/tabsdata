//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn the_signing_key_and_the_service_token_are_not_storable() {
    assert!(!Generator::JwtSigningKey.is_storable());
    assert!(!Generator::ServiceToken.is_storable());
}

#[test]
fn the_postgres_generators_are_storable() {
    assert!(Generator::PostgresSystemUser.is_storable());
    assert!(Generator::PostgresSystemPassword.is_storable());
    assert!(Generator::PostgresApiserverPassword.is_storable());
    #[cfg(feature = "enterprise")]
    assert!(Generator::PostgresAiagentPassword.is_storable());
}

#[test]
fn a_category_without_a_generator_is_storable() {
    assert!(Generator::is_storable_category("anthropic-key"));
    assert!(Generator::is_storable_category("openai-key"));
}

#[test]
fn the_non_storable_categories_are_rejected_by_name() {
    assert!(!Generator::is_storable_category("jwt_signing_key"));
    assert!(!Generator::is_storable_category("service_token"));
}

#[test]
fn an_empty_category_is_not_storable() {
    assert!(!Generator::is_storable_category(""));
}

#[test]
fn a_service_token_accepts_only_its_own_format() {
    assert!(Generator::ServiceToken.accepts(&token::generate()));
    assert!(!Generator::ServiceToken.accepts("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.e30.sig"));
    assert!(!Generator::ServiceToken.accepts(""));
}

#[test]
fn the_other_generators_accept_any_held_value() {
    assert!(Generator::JwtSigningKey.accepts("anything"));
    assert!(Generator::PostgresSystemPassword.accepts("anything"));
}
