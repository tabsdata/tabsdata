//
// Copyright 2026 Tabs Data Inc.
//

//! Wildcard listing (filename-only) + lastmod filtering, plus the `find-copy`
//! output-naming rule. Backend-agnostic — it only uses [`io::Store::list`].

use crate::transporter2::error::{TransporterError, Result};
use crate::transporter2::io::{FileMeta, Store};
use crate::transporter2::lastmod::LastMod;
use crate::transporter2::spec::Format;
use wildmatch::WildMatch;

/// Above this many files in one listing, `find` warns (the whole listing is
/// held in memory before filtering).
const LARGE_LISTING_WARN_THRESHOLD: usize = 10_000;

pub struct Found {
    pub meta: FileMeta,
    pub filename: String,
}

pub struct FindStats {
    pub found: usize,
    pub matched: usize,
    pub selected: usize,
}

/// Split a backend path into `(dir, filename)`; the wildcard lives in the
/// filename (last segment), not an intermediate one.
pub fn split_dir_file(path: &str) -> (String, String) {
    match path.rfind('/') {
        Some(i) => (path[..i].to_string(), path[i + 1..].to_string()),
        None => (String::new(), path.to_string()),
    }
}

fn filename_of(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => path[i + 1..].to_string(),
        None => path.to_string(),
    }
}

/// List the source's directory, keep names matching the wildcard, sort
/// oldest-first, cap to `max_files`, then apply the lastmod cursor (recording
/// only the files actually returned). `store.path()` carries the wildcard.
pub async fn find(
    store: &Store,
    lastmod: Option<&mut LastMod>,
    max_files: Option<usize>,
) -> Result<(Vec<Found>, FindStats)> {
    let (dir, pattern) = split_dir_file(store.path());
    if pattern.is_empty() {
        return Err(TransporterError::NoFileName(store.uri().to_string()));
    }
    let matcher = WildMatch::new(&pattern);

    let entries = store.list(&dir).await?;
    let found = entries.len();
    if found > LARGE_LISTING_WARN_THRESHOLD {
        tracing::warn!(
            "transporter: listing '{}' returned {found} files (> {LARGE_LISTING_WARN_THRESHOLD}); \
             held in memory before filtering",
            store.uri()
        );
    }

    let mut matched_vec: Vec<(FileMeta, String)> = entries
        .into_iter()
        .filter_map(|meta| {
            let filename = filename_of(&meta.path);
            matcher.matches(&filename).then_some((meta, filename))
        })
        .collect();
    let matched = matched_vec.len();
    matched_vec.sort_by_key(|(meta, _)| meta.last_modified);

    // Oldest-first. With a cursor we check-and-mark while walking, stopping once
    // `max_files` *new* files are taken (marking only those). Truncating before
    // the cursor check would keep an already-seen file in the window on resume
    // and starve newer files.
    let selected: Vec<Found> = match lastmod {
        Some(cursor) => {
            let mut out = Vec::new();
            for (meta, filename) in matched_vec {
                if max_files.is_some_and(|max| out.len() >= max) {
                    break;
                }
                if cursor.check_and_set(&pattern, &filename, meta.last_modified) {
                    out.push(Found { meta, filename });
                }
            }
            out
        }
        None => {
            if let Some(max) = max_files {
                matched_vec.truncate(max);
            }
            matched_vec
                .into_iter()
                .map(|(meta, filename)| Found { meta, filename })
                .collect()
        }
    };

    let stats = FindStats {
        found,
        matched,
        selected: selected.len(),
    };
    Ok((selected, stats))
}

// --------------------------------------------------------------------------
// find-copy output naming
// --------------------------------------------------------------------------

const COMPRESSION_EXTS: &[&str] = &["br", "bz2", "gz", "z", "lz4", "xz", "zlib", "zstd"];
const DATA_EXTS: &[&str] = &["parquet", "pq", "csv", "json", "jsonl", "ndjson", "avro", "log", "txt"];

fn canonical_data_ext(format: Format) -> &'static str {
    match format {
        Format::Parquet => "parquet",
        Format::Csv => "csv",
        Format::Ndjson => "ndjson",
        Format::Avro => "avro",
        Format::Text => "txt",
    }
}

/// Strip one trailing `.<ext>` from `name` if `<ext>` (lowercased) is in `exts`.
fn strip_one_ext<'a>(name: &'a str, exts: &[&str]) -> &'a str {
    if let Some(i) = name.rfind('.') {
        let ext = name[i + 1..].to_ascii_lowercase();
        if exts.contains(&ext.as_str()) {
            return &name[..i];
        }
    }
    name
}

/// The name a `find-copy`'d file is written under in the target dir.
///
/// * Converting (`target_format = Some`): take the source stem (strip one
///   trailing compression ext, then one trailing data ext), append the target
///   format's canonical extension, then the output compression ext (if any).
/// * Pure move (`target_format = None`): keep the source name verbatim.
pub fn output_name(
    src_filename: &str,
    target_format: Option<Format>,
    compress_ext: Option<&str>,
) -> String {
    match target_format {
        None => src_filename.to_string(),
        Some(format) => {
            let stem = strip_one_ext(strip_one_ext(src_filename, COMPRESSION_EXTS), DATA_EXTS);
            let mut name = format!("{stem}.{}", canonical_data_ext(format));
            if let Some(ext) = compress_ext {
                name.push('.');
                name.push_str(ext);
            }
            name
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn output_name_convert_strips_and_reextends() {
        // csv.gz -> parquet
        assert_eq!(output_name("data-001.csv.gz", Some(Format::Parquet), None), "data-001.parquet");
        // parquet -> csv
        assert_eq!(output_name("part.parquet", Some(Format::Csv), None), "part.csv");
        // csv -> parquet, output gzipped
        assert_eq!(
            output_name("x.csv", Some(Format::Parquet), Some("gz")),
            "x.parquet.gz"
        );
        // no recognizable data ext: just append
        assert_eq!(output_name("weird", Some(Format::Ndjson), None), "weird.ndjson");
    }

    #[test]
    fn output_name_pure_move_keeps_name() {
        assert_eq!(output_name("data-001.parquet", None, None), "data-001.parquet");
        assert_eq!(output_name("blob.bin", None, None), "blob.bin");
    }

    #[test]
    fn split_dir_file_splits_on_last_slash() {
        assert_eq!(split_dir_file("a/b/c.csv"), ("a/b".into(), "c.csv".into()));
        assert_eq!(split_dir_file("c.csv"), ("".into(), "c.csv".into()));
    }
}
