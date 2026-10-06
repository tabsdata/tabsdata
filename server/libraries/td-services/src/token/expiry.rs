//
// Copyright 2026 Tabsdata Inc.
//

use crate::token::TokenError;
use chrono::Duration;
use td_common::datetime::Timestamp;
use td_error::TdError;
use td_objects::types::basic::{AtTime, ExpiresInDays, TokenExpiresOn};
use td_security::token::config::{MaxTokenDays, ServiceTokenConfig};

/// No ceiling configured, no tokens issued.
pub fn expires_on(
    config: &ServiceTokenConfig,
    at: &AtTime,
    requested: &ExpiresInDays,
) -> Result<TokenExpiresOn, TdError> {
    let ceiling = config.max_days.ok_or(TokenError::NotConfigured)?;
    if *requested == ExpiresInDays::NEVER {
        if !ceiling.permits_never() {
            Err(TokenError::NeverExpiringNotPermitted)?
        }
        return Ok(TokenExpiresOn::never());
    }
    let days = u32::try_from(**requested).map_err(|_| TokenError::LifetimeIsNotADuration)?;
    if days == 0 {
        Err(TokenError::LifetimeIsNotADuration)?
    }
    match ceiling {
        MaxTokenDays::Unbounded => {}
        MaxTokenDays::Days(max) if days <= max => {}
        MaxTokenDays::Days(max) => Err(TokenError::AboveMaxDays(max))?,
    }
    let expires = at
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or(TokenError::LifetimeOutOfRange(days))?;
    Ok(TokenExpiresOn::from(Timestamp::from(expires)))
}

#[cfg(test)]
mod tests {
    use super::expires_on;
    use crate::token::TokenError;
    use chrono::Duration;
    use td_common::datetime::Timestamp;
    use td_error::TdError;
    use td_objects::types::basic::{AtTime, ExpiresInDays, TokenExpiresOn};
    use td_security::token::config::{MaxTokenDays, ServiceTokenConfig};

    fn config(max_days: Option<MaxTokenDays>) -> ServiceTokenConfig {
        ServiceTokenConfig { max_days }
    }

    fn days(value: i64) -> ExpiresInDays {
        ExpiresInDays::try_from(value).unwrap()
    }

    fn refusal(err: &TdError) -> &TokenError {
        err.domain_err::<TokenError>()
    }

    #[test]
    fn test_no_ceiling_configured_issues_nothing() {
        let err = expires_on(&config(None), &AtTime::default(), &days(1)).unwrap_err();
        assert!(matches!(refusal(&err), TokenError::NotConfigured));
    }

    #[test]
    fn test_a_bounded_ceiling_permits_up_to_itself() {
        let config = config(Some(MaxTokenDays::Days(90)));
        let at = AtTime::default();
        for requested in [1, 89, 90] {
            expires_on(&config, &at, &days(requested)).unwrap();
        }
        let err = expires_on(&config, &at, &days(91)).unwrap_err();
        assert!(matches!(refusal(&err), TokenError::AboveMaxDays(90)));
    }

    #[test]
    fn test_never_is_permitted_only_by_an_unbounded_ceiling() {
        let at = AtTime::default();
        let never = expires_on(
            &config(Some(MaxTokenDays::Unbounded)),
            &at,
            &ExpiresInDays::NEVER,
        )
        .unwrap();
        assert_eq!(never, TokenExpiresOn::never());

        let err = expires_on(
            &config(Some(MaxTokenDays::Days(90))),
            &at,
            &ExpiresInDays::NEVER,
        )
        .unwrap_err();
        assert!(matches!(
            refusal(&err),
            TokenError::NeverExpiringNotPermitted
        ));
    }

    #[test]
    fn test_a_lifetime_past_the_last_instant_is_refused() {
        let err = expires_on(
            &config(Some(MaxTokenDays::Unbounded)),
            &AtTime::default(),
            &days(i64::from(u32::MAX)),
        )
        .unwrap_err();
        assert!(matches!(
            refusal(&err),
            TokenError::LifetimeOutOfRange(u32::MAX)
        ));
    }

    #[test]
    fn test_zero_days_is_not_a_lifetime() {
        let err = expires_on(
            &config(Some(MaxTokenDays::Unbounded)),
            &AtTime::default(),
            &days(0),
        )
        .unwrap_err();
        assert!(matches!(refusal(&err), TokenError::LifetimeIsNotADuration));
    }

    #[test]
    fn test_the_expiry_is_the_requested_number_of_days_away() {
        let at = AtTime::default();
        let expires = expires_on(&config(Some(MaxTokenDays::Unbounded)), &at, &days(30)).unwrap();
        let expected = Timestamp::from(**at + Duration::days(30));
        assert_eq!(expires, TokenExpiresOn::from(expected));
        assert!(expires.is_live_at(&at));
    }
}
