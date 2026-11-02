//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

#[test]
fn the_signing_key_and_the_service_token_are_not_storable() {
    assert!(!Generator::JwtSigningKey.is_storable());
    assert!(!Generator::ServiceToken.is_storable());
}

#[cfg(feature = "enterprise")]
#[test]
fn each_service_identity_token_is_its_own_non_storable_token_generator() {
    for (name, expected) in [
        (
            "classifier_service_token",
            Generator::ClassifierServiceToken,
        ),
        ("embedder_service_token", Generator::EmbedderServiceToken),
    ] {
        let generator = Generator::from_name(name).unwrap();
        assert_eq!(generator, expected);
        assert_eq!(generator.name(), name);
        assert!(Generator::supported().contains(&name));
        assert!(!generator.is_storable());
        assert!(generator.accepts(&generator.generate()));
        assert!(!generator.accepts("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.e30.sig"));
    }
}

#[cfg(feature = "enterprise")]
#[test]
fn the_agent_confirm_key_is_a_storable_32_byte_base64_key() {
    let generator = Generator::from_name("aiagent_confirm_key").unwrap();
    assert_eq!(generator, Generator::AiagentConfirmKey);
    assert_eq!(generator.name(), "aiagent_confirm_key");
    assert!(Generator::supported().contains(&"aiagent_confirm_key"));
    assert!(generator.is_storable());
    let key = generator.generate();
    assert_eq!(crate::base::codec::decode(&key).unwrap().len(), 32);
    assert!(generator.accepts(&key));
    assert!(!generator.accepts("short"));
    assert_ne!(key, generator.generate());
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
