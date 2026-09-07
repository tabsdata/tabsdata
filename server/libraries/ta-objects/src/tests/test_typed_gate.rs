//
// Copyright 2026 Tabsdata Inc.
//

//! The gate invariant, encoded as a regression net: every construction path
//! of a `Typed` newtype routes through `Typed::try_new`, so each must reject
//! the same invalid input.

#[cfg(test)]
mod tests {
    use crate as ta_objects;
    use crate::typed::Typed as _;
    use td_error::{ApiError, api_error};
    use tm_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

    fn reject_x(s: String) -> Result<String, td_error::TdError> {
        if s.contains('x') {
            Err(api_error!(ApiError::InputError, "no x allowed"))?;
        }
        Ok(s)
    }

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    #[typed(min_len = 2, max_len = 5, regex = "^[a-z]+$", parser = reject_x, try_from = Ungated)]
    struct Gated(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct Ungated(String);

    const INVALID: &str = "NOT_LOWERCASE_AND_TOO_LONG";

    #[test]
    fn gate_matrix_rejects_on_every_path() {
        assert!(Gated::try_new(INVALID).is_err());
        assert!(INVALID.parse::<Gated>().is_err());
        assert!(Gated::try_from(INVALID.to_string()).is_err());
        assert!(Gated::try_from(INVALID).is_err());
        assert!(serde_json::from_str::<Gated>(&format!("{INVALID:?}")).is_err());
        let sibling = Ungated::try_new(INVALID).unwrap();
        assert!(Gated::try_from(sibling).is_err());
    }

    #[test]
    fn into_inner_roundtrips_through_the_gate() {
        let valid = Ungated::try_new("abc").unwrap();
        let gated = Gated::try_new(valid.into_inner()).unwrap();
        assert_eq!(*gated, "abc");
        let invalid = Ungated::try_new(INVALID).unwrap();
        assert!(Gated::try_new(invalid.into_inner()).is_err());
    }

    #[test]
    fn gate_matrix_accepts_valid_on_every_path() {
        assert_eq!(*Gated::try_new("abc").unwrap(), "abc");
        assert!("abc".parse::<Gated>().is_ok());
        assert!(Gated::try_from("abc").is_ok());
        assert!(serde_json::from_str::<Gated>("\"abc\"").is_ok());
        assert!(Gated::try_from(Ungated::try_new("abc").unwrap()).is_ok());
        // the custom parser is part of the gate
        assert!(Gated::try_new("axc").is_err());
    }

    // The OpenAPI schema states the rules the gate enforces.
    #[test]
    fn constrained_schema_exports_rules() {
        use utoipa::__dev::ComposeSchema;
        let schema = serde_json::to_string(&Gated::compose(vec![])).unwrap();
        assert!(schema.contains("\"minLength\":2"), "{schema}");
        assert!(schema.contains("\"maxLength\":5"), "{schema}");
        assert!(schema.contains("\"pattern\":\"^[a-z]+$\""), "{schema}");

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(secret)]
        struct GateSecret(String);
        let schema = serde_json::to_string(&GateSecret::compose(vec![])).unwrap();
        assert!(schema.contains("\"format\":\"password\""), "{schema}");

        #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[typed(min = 1, max = 5)]
        struct GateNum(i64);
        let schema = serde_json::to_string(&GateNum::compose(vec![])).unwrap();
        assert!(schema.contains("\"minimum\":1"), "{schema}");
        assert!(schema.contains("\"maximum\":5"), "{schema}");
        assert!(schema.contains("\"format\":\"int64\""), "{schema}");
    }

    // TypedEnum's emitted schema is byte-identical to what the old derived
    // utoipa ToSchema produced for a serde-renamed enum.
    #[test]
    fn enum_schema_matches_derived_output() {
        use tm_objects::TypedEnum;
        use utoipa::__dev::ComposeSchema;

        #[derive(utoipa::ToSchema, serde::Serialize)]
        #[allow(dead_code)]
        enum OldStyle {
            #[serde(rename = "P")]
            Publisher,
            Subscriber,
        }

        #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi)]
        #[allow(dead_code)]
        enum NewStyle {
            #[typed_enum(rename = "P")]
            Publisher,
            Subscriber,
        }

        assert_eq!(
            serde_json::to_string(&OldStyle::compose(vec![])).unwrap(),
            serde_json::to_string(&NewStyle::compose(vec![])).unwrap(),
        );
    }

    // A configured default that violates the constraints (via a const, which
    // the literal compile-time cross-checks can't see) panics at first use.
    const BAD_DEFAULT: &str = "X";

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    #[typed(min_len = 2, default = BAD_DEFAULT)]
    struct BadDefault(String);

    #[test]
    #[should_panic(expected = "invalid default for BadDefault")]
    fn invalid_default_panics_at_first_use() {
        let _ = BadDefault::default();
    }

    /// sqlx decode is gated too: a legacy-invalid value errors on read, while
    /// a valid one round-trips.
    #[td_test::test(db)]
    async fn decode_routes_through_the_gate(db: td_database::pool::TdPool) {
        let select_gated = async |raw: &str| -> Result<Gated, sqlx::Error> {
            let mut conn = db.begin().await.unwrap();
            sqlx::query_scalar("SELECT $1::TEXT")
                .bind(raw)
                .fetch_one(&mut *conn)
                .await
        };

        assert!(select_gated(INVALID).await.is_err());
        assert_eq!(*select_gated("abc").await.unwrap(), "abc");
    }
}
