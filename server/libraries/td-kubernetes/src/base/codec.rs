//
// Copyright 2026 Tabsdata Inc.
//

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::base::error::codec::CodecError;

pub fn encode(bytes: impl AsRef<[u8]>) -> String {
    STANDARD.encode(bytes)
}

pub fn decode(text: impl AsRef<str>) -> Result<Vec<u8>, CodecError> {
    STANDARD
        .decode(text.as_ref())
        .map_err(|error| CodecError::DecodeFailed(error.to_string()))
}
