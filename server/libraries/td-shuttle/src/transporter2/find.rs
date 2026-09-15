//
// Copyright 2026 Tabsdata Inc.
//

//! Wildcard listing (filename-only) + lastmod filtering, plus the `find-copy`
//! output-naming rule. Backend-agnostic — it only uses [`io::Store::list`].

use crate::transporter2::error::{Result, TransporterError};
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
    /// Matched files the cursor turned down -- behind the watermark, and so
    /// what a purging source deletes.
    pub purgeable: usize,
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
///
/// Returns the files to read, the matched files the cursor turned down as
/// already consumed (a purging source's delete list -- always empty without a
/// cursor, and never holding a file from below the cursor's baseline, which
/// was never read), and the counts.
pub async fn find(
    store: &Store,
    lastmod: Option<&mut LastMod>,
    max_files: Option<usize>,
) -> Result<(Vec<Found>, Vec<Found>, FindStats)> {
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
    let mut purgeable: Vec<Found> = Vec::new();
    let selected: Vec<Found> = match lastmod {
        Some(cursor) => {
            let mut out = Vec::new();
            for (meta, filename) in matched_vec {
                if max_files.is_some_and(|max| out.len() >= max) {
                    break;
                }
                if cursor.check_and_set(&pattern, &filename, meta.last_modified) {
                    out.push(Found { meta, filename });
                } else if cursor.at_or_after_initial(meta.last_modified) {
                    // Purge files that were either already consumed or are after
                    // the initial_last_modified timestamp set at function registration
                    // time.
                    purgeable.push(Found { meta, filename });
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
        purgeable: purgeable.len(),
    };
    Ok((selected, purgeable, stats))
}

// --------------------------------------------------------------------------
// find-copy output naming
// --------------------------------------------------------------------------

const COMPRESSION_EXTS: &[&str] = &["br", "bz2", "gz", "z", "lz4", "xz", "zlib", "zstd"];
const DATA_EXTS: &[&str] = &[
    "parquet", "pq", "csv", "json", "jsonl", "ndjson", "avro", "log", "txt", "xlsx",
];

fn canonical_data_ext(format: Format) -> &'static str {
    match format {
        Format::Parquet => "parquet",
        Format::Csv => "csv",
        Format::Ndjson => "ndjson",
        Format::Avro => "avro",
        Format::Text => "txt",
        Format::Excel => "xlsx",
        // Claimed by no entry in `DATA_EXTS`: a mainframe extract has no
        // conventional extension. Ebcdic is read-only, so this is only ever
        // reached for exhaustiveness.
        Format::Ebcdic => "dat",
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
    use chrono::{DateTime, Utc};

    #[test]
    fn output_name_convert_strips_and_reextends() {
        // csv.gz -> parquet
        assert_eq!(
            output_name("data-001.csv.gz", Some(Format::Parquet), None),
            "data-001.parquet"
        );
        // parquet -> csv
        assert_eq!(
            output_name("part.parquet", Some(Format::Csv), None),
            "part.csv"
        );
        // csv -> parquet, output gzipped
        assert_eq!(
            output_name("x.csv", Some(Format::Parquet), Some("gz")),
            "x.parquet.gz"
        );
        // no recognizable data ext: just append
        assert_eq!(
            output_name("weird", Some(Format::Ndjson), None),
            "weird.ndjson"
        );
    }

    #[test]
    fn output_name_turns_a_workbook_into_a_parquet() {
        assert_eq!(
            output_name("sales.xlsx", Some(Format::Parquet), None),
            "sales.parquet"
        );
        // The sheet number never reaches here -- the connector strips the uri
        // fragment before the request is written -- so a name carrying one is
        // not a case to accommodate, only evidence the strip was skipped.
        assert_eq!(
            output_name("sales.xlsx", Some(Format::Csv), Some("gz")),
            "sales.csv.gz"
        );
    }

    #[test]
    fn output_name_pure_move_keeps_name() {
        assert_eq!(
            output_name("data-001.parquet", None, None),
            "data-001.parquet"
        );
        assert_eq!(output_name("blob.bin", None, None), "blob.bin");
    }

    #[test]
    fn split_dir_file_splits_on_last_slash() {
        assert_eq!(split_dir_file("a/b/c.csv"), ("a/b".into(), "c.csv".into()));
        assert_eq!(split_dir_file("c.csv"), ("".into(), "c.csv".into()));
    }

    // --- purgeable list -------------------------------------------------
    //
    // `find` reports the matched files the cursor turned down as already
    // consumed. A file below the baseline is turned down too, but was never
    // read, so it must not appear.

    fn touch(dir: &std::path::Path, name: &str, at: DateTime<Utc>) {
        let path = dir.join(name);
        std::fs::write(&path, b"x").unwrap();
        let when = std::time::UNIX_EPOCH
            + std::time::Duration::from_secs(at.timestamp() as u64);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    async fn find_in(
        dir: &std::path::Path,
        initial: DateTime<Utc>,
        cursor: &mut Option<LastMod>,
    ) -> (Vec<String>, Vec<String>) {
        let _ = initial;
        let uri = url::Url::from_file_path(dir.join("*.csv"))
            .unwrap()
            .to_string();
        let store = crate::transporter2::io::resolve(&crate::transporter2::spec::Location {
            uri,
            configs: Default::default(),
        })
        .unwrap();
        let (selected, purgeable, _stats) = find(&store, cursor.as_mut(), None).await.unwrap();
        (
            selected.into_iter().map(|f| f.filename).collect(),
            purgeable.into_iter().map(|f| f.filename).collect(),
        )
    }

    #[tokio::test]
    async fn a_file_below_the_baseline_is_never_purgeable() {
        let dir = std::env::temp_dir().join(format!("td-find-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        touch(&dir, "old.csv", at(1_000)); // below the baseline: never read
        touch(&dir, "new.csv", at(3_000)); // at/after it: read now

        let mut cursor = Some(LastMod::new(at(2_000)));
        let (selected, purgeable) = find_in(&dir, at(2_000), &mut cursor).await;

        assert_eq!(selected, vec!["new.csv".to_string()]);
        assert!(
            purgeable.is_empty(),
            "a file the baseline excluded was never read, so it is not ours to \
             delete: {purgeable:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_file_already_consumed_is_purgeable_on_the_next_pass() {
        let dir = std::env::temp_dir().join(format!("td-find-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        touch(&dir, "a.csv", at(3_000));
        touch(&dir, "b.csv", at(4_000));

        // First pass consumes both and purges nothing -- neither is behind the
        // watermark yet.
        let mut cursor = Some(LastMod::new(at(2_000)));
        let (selected, purgeable) = find_in(&dir, at(2_000), &mut cursor).await;
        assert_eq!(selected, vec!["a.csv".to_string(), "b.csv".to_string()]);
        assert!(purgeable.is_empty(), "{purgeable:?}");

        // Second pass over the same directory: both are behind the watermark
        // now, and both were genuinely read.
        let (selected, purgeable) = find_in(&dir, at(2_000), &mut cursor).await;
        assert!(selected.is_empty(), "{selected:?}");
        assert_eq!(purgeable, vec!["a.csv".to_string(), "b.csv".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn without_a_cursor_nothing_is_purgeable() {
        let dir = std::env::temp_dir().join(format!("td-find-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        touch(&dir, "a.csv", at(1_000));

        let mut none: Option<LastMod> = None;
        let (selected, purgeable) = find_in(&dir, at(2_000), &mut none).await;

        assert_eq!(selected, vec!["a.csv".to_string()]);
        assert!(purgeable.is_empty(), "{purgeable:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

