//
// Copyright 2025 Tabs Data Inc.
//

use td_common::uri::TdUri;

/// A trait for filtering a list of URIs. Used in testing.
pub trait FilterTriggered {
    fn filter(&self, td_uris: Vec<TdUri>) -> Vec<TdUri>;
}
