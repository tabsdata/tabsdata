//
// Copyright 2025 Tabsdata Inc.
//

use tower_http::compression::CompressionLayer;

#[derive(Default)]
pub struct CompressionService;

impl CompressionService {
    pub fn layer() -> CompressionLayer {
        // By default, compresses when ACCEPT_ENCODING is present and set to
        // one of the supported encodings.
        CompressionLayer::new()
            .gzip(true)
            .deflate(true)
            .br(true)
            .zstd(true)
    }
}
