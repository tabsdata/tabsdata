//
// Copyright 2025 Tabs Data Inc.
//

use url::Url;

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct HttpRequest {
    pub method: String,
    pub url: Url,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}
