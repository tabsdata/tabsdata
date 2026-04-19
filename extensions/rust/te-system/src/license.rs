//
// Copyright 2025 Tabs Data Inc.
//

pub use ta_system::license::{
    ACCEPT_TERMS_ARGUMENT, ACCEPT_TERMS_N, ACCEPT_TERMS_NO, ACCEPT_TERMS_Y, ACCEPT_TERMS_YES,
    LicenseAcceptance,
};

pub struct TabsdataLicense;

impl LicenseAcceptance for TabsdataLicense {
    fn check_license_acceptance(&self, _accept_terms: Option<bool>) -> std::io::Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_license_acceptance_always_true() {
        let license = TabsdataLicense;
        assert!(license.check_license_acceptance(None).unwrap());
        assert!(license.check_license_acceptance(Some(true)).unwrap());
        assert!(license.check_license_acceptance(Some(false)).unwrap());
    }
}
