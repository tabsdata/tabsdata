//
// Copyright 2025. Tabs Data Inc.
//

//! Provides dynamic compression and decompression for async I/O streams.
//!
//! This module offers wrappers (`DecompressingReader` and `CompressingWriter`) that
//! dynamically apply a compression or decompression algorithm to an underlying
//! async reader or writer. The algorithm can be specified explicitly or determined
//! automatically from a file extension.
//!
//! # Examples
//!
//! Compressing and decompressing data with automatic algorithm detection:
//!

use crate::transporter::api::CompressionAlgorithmSpec;
use async_compression::tokio::bufread::{
    BrotliDecoder, BzDecoder, GzipDecoder, Lz4Decoder, XzDecoder, ZlibDecoder, ZstdDecoder,
};
use async_compression::tokio::write::{
    BrotliEncoder, BzEncoder, GzipEncoder, Lz4Encoder, XzEncoder, ZlibEncoder, ZstdEncoder,
};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use tokio::io;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader, ReadBuf};

/// Represents the compression algorithm to be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::Display)]
pub enum CompressionAlgorithm {
    /// No compression.
    #[strum(serialize = "none")]
    None,
    /// Automatically detect the algorithm from the file extension.
    #[strum(serialize = "auto")]
    Auto,
    /// Brotli compression algorithm.
    #[strum(serialize = "brotli")]
    Brotli,
    /// Bzip2 compression algorithm.
    #[strum(serialize = "bzip2")]
    Bzip,
    /// Gzip compression algorithm.
    #[strum(serialize = "gzip")]
    Gzip,
    /// LZ4 compression algorithm.
    #[strum(serialize = "lz4")]
    Lz4,
    /// XZ (LZMA) compression algorithm.
    #[strum(serialize = "xz")]
    Xz,
    /// Deflate (Zip) compression algorithm.
    #[strum(serialize = "zlib")]
    Zlib,
    /// Zstandard compression algorithm.
    #[strum(serialize = "zstd")]
    Zstd,
}

impl From<CompressionAlgorithmSpec> for CompressionAlgorithm {
    fn from(spec: CompressionAlgorithmSpec) -> Self {
        match spec {
            CompressionAlgorithmSpec::Auto => CompressionAlgorithm::Auto,
            CompressionAlgorithmSpec::Brotli => CompressionAlgorithm::Brotli,
            CompressionAlgorithmSpec::Bzip => CompressionAlgorithm::Bzip,
            CompressionAlgorithmSpec::Zip => CompressionAlgorithm::Zlib,
            CompressionAlgorithmSpec::Deflate => CompressionAlgorithm::Gzip,
            CompressionAlgorithmSpec::Gzip => CompressionAlgorithm::Gzip,
            CompressionAlgorithmSpec::Lz4 => CompressionAlgorithm::Lz4,
            CompressionAlgorithmSpec::Xz => CompressionAlgorithm::Xz,
            CompressionAlgorithmSpec::Zstd => CompressionAlgorithm::Zstd,
        }
    }
}

static COMPRESSIONS_MAP: LazyLock<HashMap<CompressionAlgorithm, Vec<&str>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert(CompressionAlgorithm::Brotli, vec!["br"]);
    map.insert(CompressionAlgorithm::Bzip, vec!["bz2"]);
    map.insert(CompressionAlgorithm::Gzip, vec!["gz", "z"]);
    map.insert(CompressionAlgorithm::Lz4, vec!["lz4"]);
    map.insert(CompressionAlgorithm::Xz, vec!["xz"]);
    map.insert(CompressionAlgorithm::Zlib, vec!["zlib"]);
    map.insert(CompressionAlgorithm::Zstd, vec!["zstd"]);
    map
});

static EXTENSIONS_TO_COMPRESSION_MAP: LazyLock<HashMap<&'static str, CompressionAlgorithm>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        for (algo, extensions) in COMPRESSIONS_MAP.iter() {
            for extension in extensions {
                map.insert(*extension, *algo);
            }
        }
        map
    });

impl CompressionAlgorithm {
    /// Returns the canonical file extension for the compression algorithm.
    ///
    /// For example, `CompressionAlgorithm::Gzip.extension()` returns `"gz"`.
    pub fn extension(&self) -> &'static str {
        COMPRESSIONS_MAP[self][0]
    }

    /// Returns all common file extensions for the compression algorithm.
    ///
    /// For example, `CompressionAlgorithm::Gzip.extensions()` returns `&["gz", "z"]`.
    pub fn extensions(&self) -> &'static [&'static str] {
        COMPRESSIONS_MAP[self].as_slice()
    }

    /// Determines the compression algorithm from a file name's extension.
    ///
    /// The extension is check is case insensitive.
    ///
    /// If the extension is not recognized, it returns `CompressionAlgorithm::None`.
    fn algorithm_by_extension(file: &str) -> Self {
        file.split('.')
            .next_back()
            .and_then(|ext| EXTENSIONS_TO_COMPRESSION_MAP.get(ext.to_lowercase().as_str()))
            .copied()
            .unwrap_or(CompressionAlgorithm::None)
    }

    /// Resolves the final compression algorithm to use based on user input and a file name.
    ///
    /// - If `compression` is `None` or `Some(CompressionAlgorithm::None)`, it returns
    ///   `CompressionAlgorithm::None`.
    /// - If `compression` is `Some(CompressionAlgorithm::Auto)`, it attempts to determine the
    ///   compression algorithm from the file extension (case insensitive).
    /// - Otherwise, it returns the specified algorithm.
    pub fn resolve_algorithm(
        compression: &Option<CompressionAlgorithm>,
        file: &str,
    ) -> CompressionAlgorithm {
        match compression {
            None | Some(CompressionAlgorithm::None) => CompressionAlgorithm::None,
            Some(CompressionAlgorithm::Auto) => Self::algorithm_by_extension(file),
            other => other.unwrap(),
        }
    }

    /// Removes the compression extension from a file name if it matches the algorithm's extensions.
    ///
    /// If the algorithm is `None`, the original file name is returned.
    /// If the algorithm is `Auto`, it determines the algorithm from the file extension first.
    pub fn remove_compression_extension<'a>(&self, file: &'a str) -> &'a str {
        if self == &CompressionAlgorithm::None {
            return file;
        }
        if self == &CompressionAlgorithm::Auto {
            let algo = Self::algorithm_by_extension(file);
            return algo.remove_compression_extension(file);
        }
        match file.split('.').next_back() {
            Some(ext) if self.extensions().contains(&ext.to_lowercase().as_str()) => {
                &file[..file.len() - ext.len() - 1]
            }
            _ => file,
        }
    }
}

pin_project! {
    #[project = DynamicDecoderProj]
    /// An internal enum to hold the specific decoder.
    enum DynamicDecoder<R: AsyncBufRead> {
        None { #[pin] inner: R },
        Brotli { #[pin] inner: BufReader<BrotliDecoder<R>> },
        Bzip { #[pin] inner: BufReader<BzDecoder<R>> },
        Gzip { #[pin] inner: BufReader<GzipDecoder<R>> },
        Lz4 { #[pin] inner: BufReader<Lz4Decoder<R>> },
        Xz { #[pin] inner: BufReader<XzDecoder<R>> },
        Zlib { #[pin] inner: BufReader<ZlibDecoder<R>> },
        Zstd { #[pin] inner: BufReader<ZstdDecoder<R>> },
    }
}

pin_project! {
    /// A wrapper that provides an `AsyncBufRead` interface over a dynamically chosen decompressor.
    ///
    /// This reader wraps an underlying reader and decompresses its contents using the specified
    /// `CompressionAlgorithm`.
    struct DecompressingReader<R: AsyncBufRead> {
        #[pin]
        inner: DynamicDecoder<R>,
    }
}

impl<R: AsyncBufRead> DecompressingReader<R> {
    /// Creates a new `DecompressingReader`.
    ///
    /// The `compression` algorithm must be a specific algorithm; `CompressionAlgorithm::Auto`
    /// is not allowed here. Use the `async_reader` factory function to handle `Auto`.
    fn new(compression: CompressionAlgorithm, reader: R) -> Self {
        let inner = match compression {
            CompressionAlgorithm::Auto => panic!("Algorithm cannot be auto at this point"),
            CompressionAlgorithm::None => DynamicDecoder::None { inner: reader },
            CompressionAlgorithm::Brotli => DynamicDecoder::Brotli {
                inner: BufReader::new(BrotliDecoder::new(reader)),
            },
            CompressionAlgorithm::Bzip => DynamicDecoder::Bzip {
                inner: BufReader::new(BzDecoder::new(reader)),
            },
            CompressionAlgorithm::Gzip => DynamicDecoder::Gzip {
                inner: BufReader::new(GzipDecoder::new(reader)),
            },
            CompressionAlgorithm::Lz4 => DynamicDecoder::Lz4 {
                inner: BufReader::new(Lz4Decoder::new(reader)),
            },
            CompressionAlgorithm::Xz => DynamicDecoder::Xz {
                inner: BufReader::new(XzDecoder::new(reader)),
            },
            CompressionAlgorithm::Zlib => DynamicDecoder::Zlib {
                inner: BufReader::new(ZlibDecoder::new(reader)),
            },
            CompressionAlgorithm::Zstd => DynamicDecoder::Zstd {
                inner: BufReader::new(ZstdDecoder::new(reader)),
            },
        };
        Self { inner }
    }
}

/// Implement `AsyncRead` by delegating to the inner decoder.
impl<R: AsyncBufRead> AsyncRead for DecompressingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project().inner.project() {
            DynamicDecoderProj::None { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Brotli { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Bzip { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Gzip { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Lz4 { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Xz { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Zlib { inner } => inner.poll_read(cx, buf),
            DynamicDecoderProj::Zstd { inner } => inner.poll_read(cx, buf),
        }
    }
}

/// Implement `AsyncBufRead` by delegating to the inner decoder.
impl<R: AsyncBufRead> AsyncBufRead for DecompressingReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        match self.project().inner.project() {
            DynamicDecoderProj::None { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Brotli { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Bzip { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Gzip { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Lz4 { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Xz { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Zlib { inner } => inner.poll_fill_buf(cx),
            DynamicDecoderProj::Zstd { inner } => inner.poll_fill_buf(cx),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        match self.project().inner.project() {
            DynamicDecoderProj::None { inner } => inner.consume(amt),
            DynamicDecoderProj::Brotli { inner } => inner.consume(amt),
            DynamicDecoderProj::Bzip { inner } => inner.consume(amt),
            DynamicDecoderProj::Gzip { inner } => inner.consume(amt),
            DynamicDecoderProj::Lz4 { inner } => inner.consume(amt),
            DynamicDecoderProj::Xz { inner } => inner.consume(amt),
            DynamicDecoderProj::Zlib { inner } => inner.consume(amt),
            DynamicDecoderProj::Zstd { inner } => inner.consume(amt),
        }
    }
}

pin_project! {
    #[project = DynamicEncoderProj]
    /// An internal enum to hold the specific encoder.
    enum DynamicEncoder<W: AsyncWrite> {
        None { #[pin] inner: W },
        Brotli { #[pin] inner: BrotliEncoder<W> },
        Bzip { #[pin] inner: BzEncoder<W> },
        Gzip { #[pin] inner: GzipEncoder<W> },
        Lz4 { #[pin] inner: Lz4Encoder<W> },
        Xz { #[pin] inner: XzEncoder<W> },
        Zlib { #[pin] inner: ZlibEncoder<W> },
        Zstd { #[pin] inner: ZstdEncoder<W> },
    }
}

pin_project! {
    /// A wrapper that provides an `AsyncWrite` interface over a dynamically chosen compressor.
    ///
    /// This writer wraps an underlying writer and compresses data written to it using the specified
    /// `CompressionAlgorithm`.
    struct CompressingWriter<W: AsyncWrite> {
        #[pin]
        inner: DynamicEncoder<W>,
    }
}

impl<W: AsyncWrite> CompressingWriter<W> {
    /// Creates a new `CompressingWriter`.
    ///
    /// The `compression` algorithm must be a specific algorithm; `CompressionAlgorithm::Auto`
    /// is not allowed here. Use the `async_writer` factory function to handle `Auto`.
    fn new(compression: CompressionAlgorithm, writer: W) -> Self {
        let inner = match compression {
            CompressionAlgorithm::Auto => panic!("Algorithm cannot be auto at this point"),
            CompressionAlgorithm::None => DynamicEncoder::None { inner: writer },
            CompressionAlgorithm::Brotli => DynamicEncoder::Brotli {
                inner: BrotliEncoder::new(writer),
            },
            CompressionAlgorithm::Bzip => DynamicEncoder::Bzip {
                inner: BzEncoder::new(writer),
            },
            CompressionAlgorithm::Gzip => DynamicEncoder::Gzip {
                inner: GzipEncoder::new(writer),
            },
            CompressionAlgorithm::Lz4 => DynamicEncoder::Lz4 {
                inner: Lz4Encoder::new(writer),
            },
            CompressionAlgorithm::Xz => DynamicEncoder::Xz {
                inner: XzEncoder::new(writer),
            },
            CompressionAlgorithm::Zlib => DynamicEncoder::Zlib {
                inner: ZlibEncoder::new(writer),
            },
            CompressionAlgorithm::Zstd => DynamicEncoder::Zstd {
                inner: ZstdEncoder::new(writer),
            },
        };
        Self { inner }
    }
}

impl<W: AsyncWrite> AsyncWrite for CompressingWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project().inner.project() {
            DynamicEncoderProj::None { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Brotli { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Bzip { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Gzip { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Lz4 { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Xz { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Zlib { inner } => inner.poll_write(cx, buf),
            DynamicEncoderProj::Zstd { inner } => inner.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project().inner.project() {
            DynamicEncoderProj::None { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Brotli { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Bzip { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Gzip { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Lz4 { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Xz { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Zlib { inner } => inner.poll_flush(cx),
            DynamicEncoderProj::Zstd { inner } => inner.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project().inner.project() {
            DynamicEncoderProj::None { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Brotli { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Bzip { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Gzip { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Lz4 { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Xz { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Zlib { inner } => inner.poll_shutdown(cx),
            DynamicEncoderProj::Zstd { inner } => inner.poll_shutdown(cx),
        }
    }
}

/// Creates an `AsyncBufRead` that decompresses data from an underlying reader.
///
/// - If `algo` is `Some(CompressionAlgorithm::Auto)`, the algorithm is inferred from the
///   file extension (case insensitive) of `file`.
/// - If `algo` is `Some` other variant, that specific algorithm is used.
/// - If `algo` is `None`, no decompression is performed.
pub fn async_reader<'a, R: AsyncBufRead>(
    algo: &Option<CompressionAlgorithm>,
    file: impl Into<&'a str>,
    r: R,
) -> impl AsyncBufRead {
    let algo = CompressionAlgorithm::resolve_algorithm(algo, file.into());
    DecompressingReader::new(algo, r)
}

/// Creates an `AsyncWrite` that compresses data and writes it to an underlying writer.
///
/// - If `algo` is `Some(CompressionAlgorithm::Auto)`, the algorithm is inferred from the
///   file extension (case insensitive) of `file`.
/// - If `algo` is `Some` other variant, that specific algorithm is used.
/// - If `algo` is `None`, no compression is performed.
pub fn async_writer<'a, W: AsyncWrite>(
    algo: &Option<CompressionAlgorithm>,
    file: impl Into<&'a str>,
    w: W,
) -> impl AsyncWrite {
    let algo = CompressionAlgorithm::resolve_algorithm(algo, file.into());
    CompressingWriter::new(algo, w)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn test_compression_algorithm(algorithm: CompressionAlgorithm, extension: &'static str) {
        let original_data = b"Hello, world! This is a test string for compression.";
        let file = format!("test.{}", extension);

        // Compress
        let mut compressed_data = Vec::new();
        {
            let algo = Some(algorithm);
            let mut writer = async_writer(&algo, &*file, &mut compressed_data);
            writer.write_all(original_data).await.unwrap();
            writer.shutdown().await.unwrap();
        }

        // Decompress
        let algo = Some(algorithm);
        let mut decompressor = async_reader(&algo, &*file, compressed_data.as_slice());
        let mut decompressed_data = Vec::new();
        decompressor
            .read_to_end(&mut decompressed_data)
            .await
            .unwrap();

        assert_eq!(original_data.as_ref(), decompressed_data.as_slice());
    }

    #[tokio::test]
    async fn test_brotli_compression() {
        test_compression_algorithm(CompressionAlgorithm::Brotli, "br").await;
    }

    #[tokio::test]
    async fn test_bzip_compression() {
        test_compression_algorithm(CompressionAlgorithm::Bzip, "bz2").await;
    }

    #[tokio::test]
    async fn test_gzip_compression() {
        test_compression_algorithm(CompressionAlgorithm::Gzip, "gz").await;
    }

    #[tokio::test]
    async fn test_lz4_compression() {
        test_compression_algorithm(CompressionAlgorithm::Lz4, "lz4").await;
    }

    #[tokio::test]
    async fn test_xz_compression() {
        test_compression_algorithm(CompressionAlgorithm::Xz, "xz").await;
    }

    #[tokio::test]
    async fn test_zip_compression() {
        test_compression_algorithm(CompressionAlgorithm::Zlib, "zlib").await;
    }

    #[tokio::test]
    async fn test_zstd_compression() {
        test_compression_algorithm(CompressionAlgorithm::Zstd, "zstd").await;
    }

    #[tokio::test]
    async fn test_no_compression() {
        test_compression_algorithm(CompressionAlgorithm::None, "txt").await;
    }

    #[tokio::test]
    async fn test_auto_compression() {
        for (extension, _) in EXTENSIONS_TO_COMPRESSION_MAP.iter() {
            let original_data = b"Hello, world! This is a test string for auto compression.";
            let file = format!("test.{}", extension);

            // Compress
            let mut compressed_data = Vec::new();
            {
                let mut writer = async_writer(
                    &Some(CompressionAlgorithm::Auto),
                    &*file,
                    &mut compressed_data,
                );
                writer.write_all(original_data).await.unwrap();
                writer.shutdown().await.unwrap();
            }

            // Decompress
            let mut decompressor = async_reader(
                &Some(CompressionAlgorithm::Auto),
                &*file,
                compressed_data.as_slice(),
            );
            let mut decompressed_data = Vec::new();
            decompressor
                .read_to_end(&mut decompressed_data)
                .await
                .unwrap();

            assert_eq!(
                original_data.as_ref(),
                decompressed_data.as_slice(),
                "Failed for extension: {}",
                extension
            );
        }
    }

    #[test]
    fn test_remove_compression_extension() {
        let tests = vec![
            (
                CompressionAlgorithm::Gzip,
                "file.txt.gz",
                "file.txt",
                "remove standard extension",
            ),
            (
                CompressionAlgorithm::Gzip,
                "file.txt.z",
                "file.txt",
                "remove alternative extension",
            ),
            (
                CompressionAlgorithm::Gzip,
                "file.gz",
                "file",
                "remove extension from file with no other extension",
            ),
            (
                CompressionAlgorithm::Gzip,
                "file.txt",
                "file.txt",
                "do not remove non-compression extension",
            ),
            (
                CompressionAlgorithm::Gzip,
                "file",
                "file",
                "handle file with no extension",
            ),
            (
                CompressionAlgorithm::None,
                "file.txt.gz",
                "file.txt.gz",
                "do not remove extension when algorithm is None",
            ),
            (
                CompressionAlgorithm::Auto,
                "file.txt.gz",
                "file.txt",
                "remove extension with Auto algorithm",
            ),
            (
                CompressionAlgorithm::Auto,
                "file.txt",
                "file.txt",
                "do not remove extension with Auto algorithm if not a compression extension",
            ),
            (
                CompressionAlgorithm::Gzip,
                "archive.tar.gz",
                "archive.tar",
                "handle multiple dots in filename",
            ),
            (
                CompressionAlgorithm::Gzip,
                ".bashrc.gz",
                ".bashrc",
                "handle dotfile",
            ),
        ];

        for (algo, file, expected, description) in tests {
            assert_eq!(
                algo.remove_compression_extension(file),
                expected,
                "Failed test: {}",
                description
            );
        }
    }
}
