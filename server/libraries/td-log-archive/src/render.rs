//
// Copyright 2026 Tabsdata Inc.
//

use bytes::Bytes;

const CONTAINER_HEADER_SEPARATOR: &str = "=";
const CONTAINER_HEADER_SEPARATOR_LEN: usize = 50;

/// The block opening each container's section, live or archived alike.
pub fn container_header(container: &str) -> Bytes {
    let line = CONTAINER_HEADER_SEPARATOR.repeat(CONTAINER_HEADER_SEPARATOR_LEN);
    Bytes::from(format!("{line}\ncontainer: {container}\n{line}\n"))
}
