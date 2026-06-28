//
// Copyright 2026 Tabs Data Inc.
//

//! The **pure** router: [`plan`] maps a fully-resolved [`CopySpec`] to a
//! [`Strategy`]. No IO, no async, no store handles — every input it reads is
//! already baked into the spec by `resolve`, which is what makes the entire
//! routing truth table unit-testable without mocks (see the table test below).

use crate::transporter2::spec::{CopySpec, Format, Locality, SchemaSource};

/// How a Convert reads its input. Phase 1 only ever produces `Sequential`;
/// `ConcurrentRanged` (N decode workers over split spans) is a Phase-2,
/// staged-file-only optimization for the splittable formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Sequential,
    ConcurrentRanged,
}

/// The execution plan for one job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Strategy {
    /// Pure byte movement (optionally re-compressing): no decode/encode.
    ByteCopy,
    /// Stream decode → encode through the convert pipeline.
    Convert { mode: ReadMode },
    /// Download → local convert → upload, for resilience on large files. The
    /// inner strategy is always a `Convert` (a job only stages because it must
    /// convert; a passthrough never stages).
    Staged(Box<Strategy>),
}

/// True when the job is a pure byte passthrough: parquet in and out, identical
/// codec, and the schema is preserved. Such a job needs no decode/encode, so it
/// is a [`Strategy::ByteCopy`] regardless of size or location.
fn is_passthrough(spec: &CopySpec) -> bool {
    spec.source.format == Format::Parquet
        && spec.target.format == Format::Parquet
        && spec.source.codec == spec.target.codec
        && matches!(spec.schema, SchemaSource::Preserve)
}

/// Choose the [`Strategy`] for `spec`. The rules, in order:
///
/// 1. **passthrough** (parquet→parquet, codec in == codec out, no schema change)
///    → [`Strategy::ByteCopy`];
/// 2. otherwise, **size ≥ staging threshold** (and staging allowed)
///    → [`Strategy::Staged`] wrapping a [`Strategy::Convert`] — the inner convert
///    runs on the large local staged file, so it uses
///    [`ReadMode::ConcurrentRanged`] for the splittable formats (CSV stays
///    [`ReadMode::Sequential`] — it is never split);
/// 3. otherwise → a [`Strategy::Convert`] whose mode is `ConcurrentRanged` when
///    the source is splittable AND (cloud-sourced OR already large), else
///    `Sequential`.
pub fn plan(spec: &CopySpec) -> Strategy {
    if is_passthrough(spec) {
        return Strategy::ByteCopy;
    }
    let splittable = spec.source.format.is_splittable();
    let large = spec.size >= spec.staging_threshold;

    if spec.allow_staging && large {
        // The inner convert reads the large *local* staged file: range-split the
        // splittable formats, sequential for CSV.
        let mode = if splittable {
            ReadMode::ConcurrentRanged
        } else {
            ReadMode::Sequential
        };
        return Strategy::Staged(Box::new(Strategy::Convert { mode }));
    }

    let cloud_source = spec.source.location.locality() == Locality::Cloud;
    let mode = if splittable && (cloud_source || large) {
        ReadMode::ConcurrentRanged
    } else {
        ReadMode::Sequential
    };
    Strategy::Convert { mode }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transporter2::spec::{Codec, Endpoint, Location, Locality, SchemaSource};
    use std::collections::HashMap;

    const THRESHOLD: u64 = 256 * 1024 * 1024;

    fn loc(locality: Locality) -> Location {
        let uri = match locality {
            Locality::Local => "file:///data/x".to_string(),
            Locality::Cloud => "s3://bucket/x".to_string(),
        };
        Location {
            uri,
            configs: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn spec(
        src_fmt: Format,
        src_codec: Codec,
        src_loc: Locality,
        tgt_fmt: Format,
        tgt_codec: Codec,
        tgt_loc: Locality,
        size: u64,
        schema: SchemaSource,
        allow_staging: bool,
    ) -> CopySpec {
        CopySpec {
            source: Endpoint {
                location: loc(src_loc),
                format: src_fmt,
                codec: src_codec,
            },
            target: Endpoint {
                location: loc(tgt_loc),
                format: tgt_fmt,
                codec: tgt_codec,
            },
            size,
            schema,
            staging_threshold: THRESHOLD,
            allow_staging,
        }
    }

    fn convert_seq() -> Strategy {
        Strategy::Convert {
            mode: ReadMode::Sequential,
        }
    }

    fn convert_ranged() -> Strategy {
        Strategy::Convert {
            mode: ReadMode::ConcurrentRanged,
        }
    }

    fn staged(mode: ReadMode) -> Strategy {
        Strategy::Staged(Box::new(Strategy::Convert { mode }))
    }

    fn staged_seq() -> Strategy {
        staged(ReadMode::Sequential)
    }

    // --- Passthrough --------------------------------------------------------

    #[test]
    fn parquet_to_parquet_same_codec_preserve_is_byte_copy_small() {
        let s = spec(
            Format::Parquet,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            1,
            SchemaSource::Preserve,
            true,
        );
        assert_eq!(plan(&s), Strategy::ByteCopy);
    }

    #[test]
    fn passthrough_is_byte_copy_even_when_huge() {
        // Passthrough wins over the size rule: an identical parquet is a byte
        // copy no matter how big (no convert ⇒ no staging needed).
        let s = spec(
            Format::Parquet,
            Codec::Zstd,
            Locality::Cloud,
            Format::Parquet,
            Codec::Zstd,
            Locality::Local,
            THRESHOLD * 10,
            SchemaSource::Preserve,
            true,
        );
        assert_eq!(plan(&s), Strategy::ByteCopy);
    }

    #[test]
    fn parquet_to_parquet_different_codec_is_convert_not_passthrough() {
        let s = spec(
            Format::Parquet,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::Zstd,
            Locality::Local,
            1,
            SchemaSource::Preserve,
            true,
        );
        assert!(matches!(plan(&s), Strategy::Convert { .. })); // not ByteCopy
    }

    #[test]
    fn parquet_to_parquet_schema_change_is_convert() {
        let s = spec(
            Format::Parquet,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            1,
            SchemaSource::Explicit,
            true,
        );
        assert!(matches!(plan(&s), Strategy::Convert { .. })); // not ByteCopy
    }

    // --- Phase 2: ConcurrentRanged selection -------------------------------

    #[test]
    fn splittable_cloud_small_is_concurrent_ranged() {
        // parquet (splittable) from the cloud, below threshold → ranged.
        let s = spec(
            Format::Parquet,
            Codec::None,
            Locality::Cloud,
            Format::Csv,
            Codec::None,
            Locality::Local,
            1,
            SchemaSource::Preserve,
            true,
        );
        assert_eq!(plan(&s), convert_ranged());
    }

    #[test]
    fn splittable_local_small_is_sequential() {
        // parquet local + small → neither cloud nor large → sequential.
        let s = spec(
            Format::Parquet,
            Codec::None,
            Locality::Local,
            Format::Csv,
            Codec::None,
            Locality::Local,
            1,
            SchemaSource::Preserve,
            true,
        );
        assert_eq!(plan(&s), convert_seq());
    }

    #[test]
    fn splittable_large_stages_with_ranged_inner() {
        // avro (splittable) large → staged, inner convert is ranged.
        let s = spec(
            Format::Avro,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            THRESHOLD,
            SchemaSource::Preserve,
            true,
        );
        assert_eq!(plan(&s), staged(ReadMode::ConcurrentRanged));
    }

    #[test]
    fn csv_large_stages_with_sequential_inner() {
        // csv is never split → staged inner stays sequential.
        let s = spec(
            Format::Csv,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            THRESHOLD,
            SchemaSource::Infer { sample_rows: 1024 },
            true,
        );
        assert_eq!(plan(&s), staged(ReadMode::Sequential));
    }

    // --- Convert vs Staged by size -----------------------------------------

    #[test]
    fn small_convert_is_sequential() {
        let s = spec(
            Format::Csv,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            THRESHOLD - 1,
            SchemaSource::Infer { sample_rows: 1024 },
            true,
        );
        assert_eq!(plan(&s), convert_seq());
    }

    #[test]
    fn large_convert_is_staged() {
        let s = spec(
            Format::Csv,
            Codec::None,
            Locality::Cloud,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            THRESHOLD,
            SchemaSource::Infer { sample_rows: 1024 },
            true,
        );
        assert_eq!(plan(&s), staged_seq());
    }

    #[test]
    fn large_convert_with_staging_disabled_is_plain_convert() {
        // The inner leg of a staging job runs with allow_staging = false, so a
        // big local temp file converts in place instead of recursively staging.
        let s = spec(
            Format::Csv,
            Codec::None,
            Locality::Local,
            Format::Parquet,
            Codec::None,
            Locality::Local,
            THRESHOLD * 5,
            SchemaSource::Infer { sample_rows: 1024 },
            false,
        );
        assert_eq!(plan(&s), convert_seq());
    }

    // --- Exhaustive matrix --------------------------------------------------

    #[test]
    fn matrix_format_codec_location_size() {
        let formats = [
            Format::Parquet,
            Format::Csv,
            Format::Ndjson,
            Format::Avro,
            Format::Text,
        ];
        let codecs = [Codec::None, Codec::Gzip, Codec::Zstd];
        let localities = [Locality::Local, Locality::Cloud];
        let sizes = [0u64, THRESHOLD - 1, THRESHOLD, THRESHOLD * 4];

        for &src_fmt in &formats {
            for &tgt_fmt in &formats {
                // Text is read-only; never a target. Skip those rows (resolve
                // rejects them before planning).
                if tgt_fmt == Format::Text {
                    continue;
                }
                for &src_codec in &codecs {
                    for &tgt_codec in &codecs {
                        for &src_loc in &localities {
                            for &tgt_loc in &localities {
                                for &size in &sizes {
                                    let s = spec(
                                        src_fmt,
                                        src_codec,
                                        src_loc,
                                        tgt_fmt,
                                        tgt_codec,
                                        tgt_loc,
                                        size,
                                        SchemaSource::Preserve,
                                        true,
                                    );
                                    let got = plan(&s);

                                    let passthrough = src_fmt == Format::Parquet
                                        && tgt_fmt == Format::Parquet
                                        && src_codec == tgt_codec;
                                    let splittable = src_fmt.is_splittable();
                                    let large = size >= THRESHOLD;
                                    let expected = if passthrough {
                                        Strategy::ByteCopy
                                    } else if large {
                                        // allow_staging = true in this matrix
                                        staged(if splittable {
                                            ReadMode::ConcurrentRanged
                                        } else {
                                            ReadMode::Sequential
                                        })
                                    } else {
                                        // not large: ranged only if splittable AND cloud-sourced
                                        let cloud = src_loc == Locality::Cloud;
                                        if splittable && cloud {
                                            convert_ranged()
                                        } else {
                                            convert_seq()
                                        }
                                    };
                                    assert_eq!(
                                        got, expected,
                                        "src={src_fmt:?}/{src_codec:?}@{src_loc:?} \
                                         tgt={tgt_fmt:?}/{tgt_codec:?}@{tgt_loc:?} size={size}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn csv_is_never_ranged() {
        // CSV is not splittable: never ConcurrentRanged, at any size/location.
        fn has_ranged(s: &Strategy) -> bool {
            match s {
                Strategy::Convert {
                    mode: ReadMode::ConcurrentRanged,
                } => true,
                Strategy::Staged(inner) => has_ranged(inner),
                _ => false,
            }
        }
        for &loc in &[Locality::Local, Locality::Cloud] {
            for size in [0u64, THRESHOLD, THRESHOLD * 9] {
                let s = spec(
                    Format::Csv,
                    Codec::None,
                    loc,
                    Format::Parquet,
                    Codec::None,
                    Locality::Local,
                    size,
                    SchemaSource::Infer { sample_rows: 1024 },
                    true,
                );
                assert!(!has_ranged(&plan(&s)), "csv must never be ranged ({loc:?}, {size})");
            }
        }
    }
}
