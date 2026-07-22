//
// Copyright 2026 Tabsdata Inc.
//

//! Codec policy: map the request-level [`CompressionAlgorithmSpec`] to a
//! resolved [`Codec`], drive the (de)compression byte wrappers, and provide a
//! magic-byte sanity guard.
//!
//! Codec is **declared** (an explicit algorithm, or `auto` resolved against the
//! file extension) — the pipeline never sniffs to *detect* a codec. The
//! magic-byte check below is only a guard inside decode to fail loudly on a
//! mislabeled file, per module invariant 2.
//!
//! The actual streaming (de)compression reuses the generic, well-tested
//! wrappers from [`crate::transporter::compression`] (a format-agnostic utility);
//! transporter owns only the policy on top.

use crate::transporter::api::CompressionAlgorithmSpec;
use crate::transporter::compression::{CompressionAlgorithm, async_reader, async_writer};
use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::spec::Codec;
use bytes::Bytes;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

/// Resolve a request codec spec against `filename`. `None` ⇒ uncompressed;
/// `auto` ⇒ infer from the file extension (unknown extension ⇒ uncompressed,
/// matching the legacy transporter); any explicit spec ⇒ that codec.
pub fn resolve(spec: Option<CompressionAlgorithmSpec>, filename: &str) -> Codec {
    match spec {
        None => Codec::None,
        Some(CompressionAlgorithmSpec::Auto) => from_extension(filename),
        Some(other) => from_spec(other),
    }
}

/// Map a concrete (non-`auto`) spec to a [`Codec`].
fn from_spec(spec: CompressionAlgorithmSpec) -> Codec {
    match spec {
        // `auto` is resolved by `resolve`; treat a stray one as uncompressed.
        CompressionAlgorithmSpec::Auto => Codec::None,
        CompressionAlgorithmSpec::Brotli => Codec::Brotli,
        CompressionAlgorithmSpec::Bzip => Codec::Bzip,
        CompressionAlgorithmSpec::Zip => Codec::Zlib,
        CompressionAlgorithmSpec::Deflate => Codec::Gzip,
        CompressionAlgorithmSpec::Gzip => Codec::Gzip,
        CompressionAlgorithmSpec::Lz4 => Codec::Lz4,
        CompressionAlgorithmSpec::Xz => Codec::Xz,
        CompressionAlgorithmSpec::Zstd => Codec::Zstd,
    }
}

/// The canonical file extension for a codec (without the dot), or `None` for
/// [`Codec::None`]. Used to name compressed outputs in `find-copy`.
pub fn extension(codec: Codec) -> Option<&'static str> {
    match codec {
        Codec::None => None,
        Codec::Brotli => Some("br"),
        Codec::Bzip => Some("bz2"),
        Codec::Gzip => Some("gz"),
        Codec::Lz4 => Some("lz4"),
        Codec::Xz => Some("xz"),
        Codec::Zlib => Some("zlib"),
        Codec::Zstd => Some("zstd"),
    }
}

/// Infer a codec from a file's trailing extension. A non-compression extension
/// (`.csv`, none, ...) maps to [`Codec::None`].
pub fn from_extension(filename: &str) -> Codec {
    match filename
        .rsplit('.')
        .next()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("br") => Codec::Brotli,
        Some("bz2") => Codec::Bzip,
        Some("gz" | "z") => Codec::Gzip,
        Some("lz4") => Codec::Lz4,
        Some("xz") => Codec::Xz,
        Some("zlib") => Codec::Zlib,
        Some("zstd") => Codec::Zstd,
        _ => Codec::None,
    }
}

/// Bridge to the generic compression wrappers' algorithm enum. `io` / `convert`
/// pass the result (in a `Some(_)`) to
/// [`async_reader`](crate::transporter::compression::async_reader) /
/// [`async_writer`](crate::transporter::compression::async_writer) to (de)compress
/// the byte streams; resolving the codec here keeps that policy out of the IO layer.
pub fn to_algorithm(codec: Codec) -> CompressionAlgorithm {
    match codec {
        Codec::None => CompressionAlgorithm::None,
        Codec::Brotli => CompressionAlgorithm::Brotli,
        Codec::Bzip => CompressionAlgorithm::Bzip,
        Codec::Gzip => CompressionAlgorithm::Gzip,
        Codec::Lz4 => CompressionAlgorithm::Lz4,
        Codec::Xz => CompressionAlgorithm::Xz,
        Codec::Zlib => CompressionAlgorithm::Zlib,
        Codec::Zstd => CompressionAlgorithm::Zstd,
    }
}

/// Decompress a whole in-memory object with `codec` (no-op for [`Codec::None`]).
/// The reader is built + consumed entirely within this scope, so the generic
/// wrappers' lifetime quirk (see module note) does not apply.
pub async fn decompress_all(codec: Codec, data: Bytes) -> Result<Bytes> {
    if codec == Codec::None {
        return Ok(data);
    }
    let algo = Some(to_algorithm(codec));
    let reader = async_reader(&algo, "", BufReader::new(data.as_ref()));
    tokio::pin!(reader);
    let mut out = Vec::new();
    reader
        .read_to_end(&mut out)
        .await
        .map_err(TransporterError::from)?;
    Ok(Bytes::from(out))
}

/// Compress a whole in-memory object with `codec` (no-op for [`Codec::None`]).
pub async fn compress_all(codec: Codec, data: Bytes) -> Result<Bytes> {
    if codec == Codec::None {
        return Ok(data);
    }
    let algo = Some(to_algorithm(codec));
    let mut out = Vec::new();
    {
        let writer = async_writer(&algo, "", &mut out);
        tokio::pin!(writer);
        writer
            .write_all(&data)
            .await
            .map_err(TransporterError::from)?;
        writer.shutdown().await.map_err(TransporterError::from)?;
    }
    Ok(Bytes::from(out))
}

/// Stream-decompress a local file `src` → `dst` with `codec` (bounded memory).
pub async fn decompress_file(codec: Codec, src: &Path, dst: &Path) -> Result<()> {
    let input = tokio::fs::File::open(src)
        .await
        .map_err(TransporterError::from)?;
    let algo = Some(to_algorithm(codec));
    let reader = async_reader(&algo, "", BufReader::new(input));
    tokio::pin!(reader);
    let mut out = tokio::fs::File::create(dst)
        .await
        .map_err(TransporterError::from)?;
    tokio::io::copy(&mut reader, &mut out)
        .await
        .map_err(TransporterError::from)?;
    out.shutdown().await.map_err(TransporterError::from)?;
    Ok(())
}

/// Stream-compress a local file `src` → `dst` with `codec` (bounded memory).
pub async fn compress_file(codec: Codec, src: &Path, dst: &Path) -> Result<()> {
    let mut input = BufReader::new(
        tokio::fs::File::open(src)
            .await
            .map_err(TransporterError::from)?,
    );
    let algo = Some(to_algorithm(codec));
    let out = tokio::fs::File::create(dst)
        .await
        .map_err(TransporterError::from)?;
    let writer = async_writer(&algo, "", out);
    tokio::pin!(writer);
    tokio::io::copy(&mut input, &mut writer)
        .await
        .map_err(TransporterError::from)?;
    writer.shutdown().await.map_err(TransporterError::from)?;
    Ok(())
}

/// Magic-byte sanity guard: does `head` plausibly start a `codec` stream?
/// Returns `true` for codecs without a stable magic (or [`Codec::None`]); a
/// `false` should be surfaced as [`TransporterError::CodecMismatch`](crate::transporter2::error::TransporterError::CodecMismatch).
pub fn magic_ok(codec: Codec, head: &[u8]) -> bool {
    match codec {
        Codec::None => true,
        Codec::Gzip => head.starts_with(&[0x1f, 0x8b]),
        Codec::Bzip => head.starts_with(b"BZh"),
        Codec::Xz => head.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]),
        Codec::Zstd => head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]),
        Codec::Zlib => head.first().is_some_and(|b| b & 0x0f == 0x08), // CMF: deflate
        // brotli has no magic; lz4 frame magic exists but the legacy stream
        // wrapper is not framed — don't guard these.
        Codec::Brotli | Codec::Lz4 => true,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn resolve_none_and_explicit() {
        assert_eq!(resolve(None, "x.gz"), Codec::None);
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Gzip), "x"),
            Codec::Gzip
        );
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Zip), "x"),
            Codec::Zlib
        );
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Deflate), "x"),
            Codec::Gzip
        );
    }

    #[test]
    fn resolve_auto_from_extension() {
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Auto), "a.csv.gz"),
            Codec::Gzip
        );
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Auto), "a.parquet.zstd"),
            Codec::Zstd
        );
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Auto), "a.csv"),
            Codec::None
        );
        assert_eq!(
            resolve(Some(CompressionAlgorithmSpec::Auto), "a.BZ2"),
            Codec::Bzip
        );
    }

    #[test]
    fn magic_guard() {
        assert!(magic_ok(Codec::Gzip, &[0x1f, 0x8b, 0x08]));
        assert!(!magic_ok(Codec::Gzip, b"PK\x03\x04"));
        assert!(magic_ok(Codec::None, b"anything"));
        assert!(magic_ok(Codec::Zstd, &[0x28, 0xb5, 0x2f, 0xfd, 0x00]));
    }
}
