//
// Copyright 2025 Tabs Data Inc.
//

use ta_tableframe::execution::test_utils::FilterTriggered;
use td_common::uri::TdUri;

pub struct TdUriFilter;

impl FilterTriggered for TdUriFilter {
    fn filter(&self, td_uris: Vec<TdUri>) -> Vec<TdUri> {
        let first = td_uris.into_iter().next();
        match first {
            Some(uri) => vec![uri],
            None => vec![],
        }
    }
}
