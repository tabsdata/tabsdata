//
// Copyright 2026 Tabsdata Inc.
//

//! One fixture-struct pair per tdx system type, mirroring
//! `tabsdatax.itest.fixture.backends`.
//!
//! The object-store backends (`s3`, `gcs`, `azureblob`) share the prefix / URL /
//! store plumbing below: a system is a bucket-or-container plus a staging-root
//! prefix (`SYS_PATH`), a domain is one prefix under it (`BASE_PATH`, named
//! `PREFIX`), and [`store_of`] hands out a store rooted at the container, so the
//! `*_prefix` helpers are the paths into it. `fs` is the odd one out — a local
//! root is a real directory with nothing above it — and deals in `PathBuf`s.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use url::Url;

pub mod azureblob;
pub mod fs;
pub mod gcs;
pub mod s3;

pub use azureblob::{AzureblobDomain, AzureblobSystem};
pub use fs::{FsDomain, FsSystem};
pub use gcs::{GcsDomain, GcsSystem};
pub use s3::{S3Domain, S3System};

/// The `object_store` config a fixture struct hands out.
///
/// Blank credentials are dropped rather than passed through, so an empty
/// `S3__ACCESS_KEY` falls back to the cloud's default credential chain — the
/// same contract the Python backends have with their SDKs.
pub(crate) fn options<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> HashMap<String, String> {
    pairs
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// A descriptor path as a slash-separated object-store prefix, with no leading
/// or trailing separator.
pub(crate) fn as_prefix(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string()
}

/// `<base>/<child>`, either side possibly empty.
pub(crate) fn join_prefix(base: &str, child: &str) -> String {
    let child = child.trim_matches('/');
    match (base.is_empty(), child.is_empty()) {
        (true, _) => child.to_string(),
        (_, true) => base.to_string(),
        _ => format!("{base}/{child}"),
    }
}

/// `<scheme>://<container>/<prefix>`, where `container` is the bucket for `s3` /
/// `gs` and the blob container for `az`.
pub(crate) fn url_of(scheme: &str, container: &str, prefix: &str) -> Result<Url, String> {
    let uri = if prefix.is_empty() {
        format!("{scheme}://{container}")
    } else {
        format!("{scheme}://{container}/{prefix}")
    };
    Url::parse(&uri).map_err(|e| format!("cannot build a '{scheme}' url from '{uri}': {e}"))
}

/// The store `url` addresses, rooted at its container — the `*_prefix` helpers
/// are the paths into it, never relative to the domain.
pub(crate) fn store_of(
    url: &Url,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, String> {
    object_store::parse_url_opts(url, options)
        .map(|(store, _)| Arc::from(store))
        .map_err(|e| format!("cannot open the object store at {url}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_options_drop_blanks() {
        let opts = options([("a", "1"), ("b", ""), ("c", "3")]);
        assert_eq!(opts["a"], "1");
        assert_eq!(opts["c"], "3");
        assert!(!opts.contains_key("b"));
    }

    #[test]
    fn test_as_prefix() {
        assert_eq!(as_prefix(&PathBuf::from("/root/dom/")), "root/dom");
        assert_eq!(as_prefix(&PathBuf::from("root\\dom")), "root/dom");
        assert_eq!(as_prefix(&PathBuf::from("")), "");
    }

    #[test]
    fn test_join_prefix() {
        assert_eq!(join_prefix("root", "csv/a.csv"), "root/csv/a.csv");
        assert_eq!(join_prefix("root", "/csv/"), "root/csv");
        assert_eq!(join_prefix("", "csv"), "csv");
        assert_eq!(join_prefix("root", ""), "root");
    }

    #[test]
    fn test_url_of() {
        assert_eq!(url_of("s3", "b", "p").unwrap().as_str(), "s3://b/p");
        assert_eq!(url_of("gs", "b", "").unwrap().as_str(), "gs://b");
        assert_eq!(url_of("az", "c", "p").unwrap().as_str(), "az://c/p");
    }
}
