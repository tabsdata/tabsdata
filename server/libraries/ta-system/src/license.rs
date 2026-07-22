//
// Copyright 2026 Tabsdata Inc.
//

pub const ACCEPT_TERMS_ARGUMENT: &str = "--accept-terms";
pub const ACCEPT_TERMS_Y: &str = "y";
pub const ACCEPT_TERMS_YES: &str = "yes";
pub const ACCEPT_TERMS_N: &str = "n";
pub const ACCEPT_TERMS_NO: &str = "no";

pub trait LicenseAcceptance {
    fn check_license_acceptance(&self, accept_terms: Option<bool>) -> std::io::Result<bool>;
}
