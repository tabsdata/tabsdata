//
// Copyright 2025. Tabs Data Inc.
//

use crate::transporter::api::{CopyType, FileCopyReport, Location};
use crate::transporter::compression::{CompressionAlgorithm, async_reader, async_writer};
use crate::transporter::error::TransporterError;
use crate::transporter::retry::with_retry;
use crate::transporter::taskrunner::AsyncLambda;
use bytes::{BufMut, BytesMut};
use futures::{Future, stream::TryStreamExt};
use object_store::path::Path;
use object_store::{
    GetResult, MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload, UploadPart,
};
use std::pin::Pin;
use std::task::{Context, Poll};
use td_common::time::UniqueUtc;
use td_error::td_error;
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::io::StreamReader;
use url::Url;

#[allow(clippy::too_many_arguments)]
pub fn copy_lambda(
    idx: usize,
    source: Location<Url>,
    source_store: Box<dyn ObjectStore>,
    source_path: Path,
    source_compression: Option<CompressionAlgorithm>,
    target: Location<Url>,
    target_store: Box<dyn ObjectStore>,
    target_path: Path,
    target_compression: Option<CompressionAlgorithm>,
) -> AsyncLambda<FileCopyReport, TransporterError> {
    let buffer_size = target.buffer_size();

    Box::pin(async move {
        let start = UniqueUtc::now_millis();
        let written_bytes = with_retry("copy_compression", || async {
            compression_copy(
                &source_store,
                &source_path,
                source_compression,
                &target_store,
                &target_path,
                target_compression,
                buffer_size,
            )
            .await
        })
        .await
        .map_err(TransporterError::from)?;
        let end = UniqueUtc::now_millis();
        let report = FileCopyReport {
            copy_type: CopyType::Compression,
            idx,
            from: source.url(),
            size: written_bytes,
            to: target.url(),
            started_at: start,
            ended_at: end,
        };
        Ok(report)
    })
}

fn get_result_to_async_read(get_result: GetResult) -> impl AsyncRead {
    // Convert the GetResult into a stream of bytes.
    // The stream is adapted to an AsyncRead using StreamReader.
    // We need to map the error type from object_store::Error to std::io::Error.
    let stream = get_result.into_stream().map_err(std::io::Error::other);
    StreamReader::new(stream)
}

/// State machine for shutdown process
enum ShutdownState {
    Writing,
    FlushingLastPart,
    Completing,
    Done,
}

/// Wraps a MultipartUpload to implement AsyncWrite
struct MultipartWriter {
    upload: Option<Box<dyn MultipartUpload>>,
    buffer: BytesMut,
    min_part_size: usize,
    writing: Option<UploadPart>,
    complete_fut: Option<
        Pin<
            Box<
                dyn std::future::Future<Output = object_store::Result<object_store::PutResult>>
                    + Send,
            >,
        >,
    >,
    shutdown_state: ShutdownState,
}

impl MultipartWriter {
    /// Create a new MultipartWriter with specified minimum part size
    fn with_part_size(upload: Box<dyn MultipartUpload>, min_part_size: usize) -> Self {
        Self {
            upload: Some(upload),
            buffer: BytesMut::with_capacity(min_part_size),
            min_part_size,
            writing: None,
            complete_fut: None,
            shutdown_state: ShutdownState::Writing,
        }
    }

    /// Flush the current buffer as a part
    fn flush_buffer(&mut self) -> Result<(), io::Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| io::Error::other("upload already completed"))?;

        let data = self.buffer.split().freeze();
        let payload = PutPayload::from(data);
        let fut = upload.put_part(payload);
        self.writing = Some(fut);
        Ok(())
    }
}

impl AsyncWrite for MultipartWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // First, check if we have a pending write
        if let Some(mut writing) = self.writing.take() {
            match Pin::new(&mut writing).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    // Write completed, continue
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(io::Error::other(format!(
                        "multipart upload error: {}",
                        e
                    ))));
                }
                Poll::Pending => {
                    // Put the future back and return pending
                    self.writing = Some(writing);
                    return Poll::Pending;
                }
            }
        }

        // Add data to buffer
        let to_write = buf.len().min(self.min_part_size - self.buffer.len());
        self.buffer.put_slice(&buf[..to_write]);

        // If buffer is full, flush it
        if self.buffer.len() >= self.min_part_size {
            self.flush_buffer()?;
            // Poll the future immediately to start the upload
            if let Some(mut writing) = self.writing.take() {
                match Pin::new(&mut writing).poll(cx) {
                    Poll::Ready(Ok(_)) => {
                        // Write completed immediately
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(io::Error::other(format!(
                            "multipart upload error: {}",
                            e
                        ))));
                    }
                    Poll::Pending => {
                        self.writing = Some(writing);
                    }
                }
            }
        }

        Poll::Ready(Ok(to_write))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // Poll any pending write
        if let Some(mut writing) = self.writing.take() {
            match Pin::new(&mut writing).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    // Write completed
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(io::Error::other(format!(
                        "multipart upload error: {}",
                        e
                    ))));
                }
                Poll::Pending => {
                    self.writing = Some(writing);
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        loop {
            match self.shutdown_state {
                ShutdownState::Writing => {
                    // First, ensure any pending write is complete
                    if let Some(mut writing) = self.writing.take() {
                        match Pin::new(&mut writing).poll(cx) {
                            Poll::Ready(Ok(_)) => {}
                            Poll::Ready(Err(e)) => {
                                return Poll::Ready(Err(io::Error::other(format!(
                                    "multipart upload error: {}",
                                    e
                                ))));
                            }
                            Poll::Pending => {
                                self.writing = Some(writing);
                                return Poll::Pending;
                            }
                        }
                    }

                    // If we have buffered data, flush it
                    if !self.buffer.is_empty() {
                        self.flush_buffer()?;
                        tracing::trace!("shuttle.copy_compression: Writing -> FlushingLastPart");
                        self.shutdown_state = ShutdownState::FlushingLastPart;
                        continue;
                    }

                    // No buffered data, move to completing
                    tracing::trace!("shuttle.copy_compression: Writing -> Completing");
                    self.shutdown_state = ShutdownState::Completing;
                    continue;
                }
                ShutdownState::FlushingLastPart => {
                    // Poll the last part write
                    if let Some(mut writing) = self.writing.take() {
                        match Pin::new(&mut writing).poll(cx) {
                            Poll::Ready(Ok(_)) => {
                                // Last part complete, move to completing
                                tracing::trace!(
                                    "shuttle.copy_compression: FlushingLastPart -> Completing"
                                );
                                self.shutdown_state = ShutdownState::Completing;
                                continue;
                            }
                            Poll::Ready(Err(e)) => {
                                return Poll::Ready(Err(io::Error::other(format!(
                                    "multipart upload error: {}",
                                    e
                                ))));
                            }
                            Poll::Pending => {
                                self.writing = Some(writing);
                                return Poll::Pending;
                            }
                        }
                    }

                    // No write in progress, move to completing
                    tracing::trace!(
                        "shuttle.copy_compression: FlushingLastPart -> Completing (no write)"
                    );
                    self.shutdown_state = ShutdownState::Completing;
                    continue;
                }
                ShutdownState::Completing => {
                    // Create the complete future if we haven't already
                    if self.complete_fut.is_none() {
                        let mut upload = self
                            .upload
                            .take()
                            .ok_or_else(|| io::Error::other("upload already completed"))?;

                        // Call complete() and box the future immediately
                        let fut = Box::pin(async move { upload.complete().await });

                        self.complete_fut = Some(fut);
                    }

                    // Poll the complete future
                    if let Some(fut) = self.complete_fut.as_mut() {
                        match fut.as_mut().poll(cx) {
                            Poll::Ready(Ok(_)) => {
                                tracing::trace!("shuttle.copy_compression: Completing -> Done");
                                self.shutdown_state = ShutdownState::Done;
                                return Poll::Ready(Ok(()));
                            }
                            Poll::Ready(Err(e)) => {
                                return Poll::Ready(Err(io::Error::other(format!(
                                    "multipart complete error: {}",
                                    e
                                ))));
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }

                    return Poll::Ready(Err(io::Error::other("complete future missing")));
                }
                ShutdownState::Done => {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

#[td_error]
pub enum CompressionCopyError {
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("Object store error: {0}")]
    IoError(#[from] io::Error),
}

async fn compression_copy(
    source_store: &dyn ObjectStore,
    source_path: &Path,
    source_compression: Option<CompressionAlgorithm>,
    dest_store: &dyn ObjectStore,
    dest_path: &Path,
    dest_compression: Option<CompressionAlgorithm>,
    dest_buffer_size: usize,
) -> Result<u64, CompressionCopyError> {
    // create async reader with decompression if needed
    let get_result = source_store.get(source_path).await?;
    let reader = get_result_to_async_read(get_result);
    let reader = BufReader::new(reader);
    let file = source_path.to_string();
    let mut reader = async_reader(&source_compression, file.as_str(), reader);

    // create async writer with compression if needed
    let uploader = dest_store.put_multipart(dest_path).await?;
    let writer = MultipartWriter::with_part_size(uploader, dest_buffer_size);
    let file = dest_path.to_string();
    let mut writer = async_writer(&dest_compression, file.as_str(), writer);

    // 3. Use tokio's copy_buf to drive the stream from source to destination.
    let written_bytes = tokio::io::copy_buf(&mut reader, &mut writer).await?;
    writer.shutdown().await?;

    Ok(written_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::bufread::{
        BrotliDecoder, BrotliEncoder, BzDecoder, BzEncoder, GzipDecoder, GzipEncoder, Lz4Decoder,
        Lz4Encoder, XzDecoder, XzEncoder, ZlibDecoder, ZlibEncoder, ZstdDecoder, ZstdEncoder,
    };
    use object_store::local::LocalFileSystem;
    use std::fs;
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    async fn test_copy(
        source_compression: Option<CompressionAlgorithm>,
        source_ext: &str,
        dest_compression: Option<CompressionAlgorithm>,
        dest_ext: &str,
    ) {
        let path = td_common::tdtestdir!();
        let source_store = LocalFileSystem::new();
        let dest_store = LocalFileSystem::new();
        let source_file = format!("source.{}", source_ext);
        let source_path = path.join(&source_file);
        let dest_file = format!("dest.{}", dest_ext);
        let dest_path = path.join(&dest_file);

        let resolved_source_compression = CompressionAlgorithm::resolve_algorithm(
            &source_compression,
            source_path.to_str().unwrap(),
        );
        let resolved_dest_compression =
            CompressionAlgorithm::resolve_algorithm(&dest_compression, dest_path.to_str().unwrap());

        let source_data =
            b"hello worldhello worldhello worldhello worldhello worldhello worldhello world"
                .to_vec();
        let source_data_to_assert = source_data.clone();

        let source_file_data: Vec<u8> = match resolved_source_compression {
            CompressionAlgorithm::None => source_data,
            CompressionAlgorithm::Brotli => {
                run_codec(source_data, BrotliEncoder::new).await.unwrap()
            }
            CompressionAlgorithm::Bzip => run_codec(source_data, BzEncoder::new).await.unwrap(),
            CompressionAlgorithm::Gzip => run_codec(source_data, GzipEncoder::new).await.unwrap(),
            CompressionAlgorithm::Lz4 => run_codec(source_data, Lz4Encoder::new).await.unwrap(),
            CompressionAlgorithm::Xz => run_codec(source_data, XzEncoder::new).await.unwrap(),
            CompressionAlgorithm::Zlib => run_codec(source_data, ZlibEncoder::new).await.unwrap(),
            CompressionAlgorithm::Zstd => run_codec(source_data, ZstdEncoder::new).await.unwrap(),
            CompressionAlgorithm::Auto => {
                panic!("Cannot be Auto")
            }
        };
        fs::write(&source_path, source_file_data).unwrap();

        compression_copy(
            &source_store,
            &Path::from_absolute_path(&source_path).unwrap(),
            Some(resolved_source_compression),
            &dest_store,
            &Path::from_absolute_path(&dest_path).unwrap(),
            Some(resolved_dest_compression),
            10,
        )
        .await
        .unwrap();

        let dest_file_data = fs::read(&dest_path).unwrap();

        let dest_data: Vec<u8> = match resolved_dest_compression {
            CompressionAlgorithm::None => dest_file_data,
            CompressionAlgorithm::Brotli => {
                run_codec(dest_file_data, BrotliDecoder::new).await.unwrap()
            }
            CompressionAlgorithm::Bzip => run_codec(dest_file_data, BzDecoder::new).await.unwrap(),
            CompressionAlgorithm::Gzip => {
                run_codec(dest_file_data, GzipDecoder::new).await.unwrap()
            }
            CompressionAlgorithm::Lz4 => run_codec(dest_file_data, Lz4Decoder::new).await.unwrap(),
            CompressionAlgorithm::Xz => run_codec(dest_file_data, XzDecoder::new).await.unwrap(),
            CompressionAlgorithm::Zlib => {
                run_codec(dest_file_data, ZlibDecoder::new).await.unwrap()
            }
            CompressionAlgorithm::Zstd => {
                run_codec(dest_file_data, ZstdDecoder::new).await.unwrap()
            }
            CompressionAlgorithm::Auto => {
                panic!("Cannot be Auto")
            }
        };

        assert_eq!(dest_data, source_data_to_assert);

        if source_compression == Some(CompressionAlgorithm::Auto) {
            assert_eq!(
                resolved_source_compression,
                CompressionAlgorithm::resolve_algorithm(
                    &Some(CompressionAlgorithm::Auto),
                    source_ext
                )
            );
        }
        if dest_compression == Some(CompressionAlgorithm::Auto) {
            assert_eq!(
                resolved_dest_compression,
                CompressionAlgorithm::resolve_algorithm(
                    &Some(CompressionAlgorithm::Auto),
                    dest_ext
                )
            );
        }
    }

    async fn run_codec<E>(
        data: Vec<u8>,
        encoder_fn: impl FnOnce(BufReader<Cursor<Vec<u8>>>) -> E,
    ) -> Result<Vec<u8>, std::io::Error>
    where
        E: tokio::io::AsyncRead + Unpin,
    {
        let reader = BufReader::new(Cursor::new(data));
        let mut encoder = encoder_fn(reader);

        let mut compressed = Vec::new();
        encoder.read_to_end(&mut compressed).await?;

        Ok(compressed)
    }

    #[tokio::test]
    async fn test_copy_none_none() {
        test_copy(None, "txt", None, "txt").await;
    }

    #[tokio::test]
    async fn test_copy_none_algo_none_algo() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_brotli() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Brotli),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_bzip2() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Bzip),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_gzip() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Gzip),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_lz4() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Lz4),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_xz() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Xz),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_zlib() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Zlib),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_none_zstd() {
        test_copy(
            Some(CompressionAlgorithm::None),
            "txt",
            Some(CompressionAlgorithm::Zstd),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_brotli_none() {
        test_copy(
            Some(CompressionAlgorithm::Brotli),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_bzip2_none() {
        test_copy(
            Some(CompressionAlgorithm::Bzip),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_gzip_none() {
        test_copy(
            Some(CompressionAlgorithm::Gzip),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_lz4_none() {
        test_copy(
            Some(CompressionAlgorithm::Lz4),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_xz_none() {
        test_copy(
            Some(CompressionAlgorithm::Xz),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_zlib_none() {
        test_copy(
            Some(CompressionAlgorithm::Zlib),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_zstd_none() {
        test_copy(
            Some(CompressionAlgorithm::Zstd),
            "txt",
            Some(CompressionAlgorithm::None),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_brotli_zlib() {
        test_copy(
            Some(CompressionAlgorithm::Brotli),
            "txt",
            Some(CompressionAlgorithm::Zlib),
            "txt",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_auto_zip() {
        test_copy(
            Some(CompressionAlgorithm::Auto),
            "brotli",
            Some(CompressionAlgorithm::Auto),
            "zip",
        )
        .await;
    }

    #[tokio::test]
    async fn test_copy_lambda_success() {
        let source_compression = Some(CompressionAlgorithm::Gzip);
        let source_ext = "gz";
        let dest_compression = Some(CompressionAlgorithm::Brotli);
        let dest_ext = "br";

        let path = td_common::tdtestdir!();
        let source_store = Box::new(LocalFileSystem::new());
        let dest_store = Box::new(LocalFileSystem::new());
        let source_file = format!("source.{}", source_ext);
        let source_path = path.join(&source_file);
        let dest_file = format!("dest.{}", dest_ext);
        let dest_path = path.join(&dest_file);

        let source_url: Url = Url::from_file_path(&source_path).unwrap();
        let dest_url: Url = Url::from_file_path(&dest_path).unwrap();

        let source_data = b"hello world from copy_lambda test. this is longer to test buffering."
            .repeat(5)
            .to_vec();
        let source_data_to_assert = source_data.clone();

        let resolved_source_compression =
            CompressionAlgorithm::resolve_algorithm(&source_compression, source_url.as_ref());

        let source_file_data: Vec<u8> = match resolved_source_compression {
            CompressionAlgorithm::Gzip => run_codec(source_data, GzipEncoder::new).await.unwrap(),
            _ => panic!("Unsupported compression for test setup"),
        };
        fs::write(&source_path, source_file_data).unwrap();

        let source_loc = Location::LocalFile {
            url: source_url.clone(),
        };
        let dest_loc = Location::LocalFile {
            url: dest_url.clone(),
        };

        let lambda = copy_lambda(
            1,
            source_loc,
            source_store,
            Path::from_absolute_path(&source_path).unwrap(),
            source_compression,
            dest_loc,
            dest_store,
            Path::from_absolute_path(&dest_path).unwrap(),
            dest_compression,
        );

        let report = lambda.await.unwrap();

        assert_eq!(report.idx, 1);
        assert_eq!(report.from, source_url);
        assert_eq!(report.to, dest_url);
        assert_eq!(report.size, source_data_to_assert.len() as u64);
        assert_eq!(report.copy_type, CopyType::Compression);
        assert!(report.started_at <= report.ended_at);

        let dest_file_data = fs::read(&dest_path).unwrap();
        let resolved_dest_compression =
            CompressionAlgorithm::resolve_algorithm(&dest_compression, dest_path.to_str().unwrap());

        let dest_data: Vec<u8> = match resolved_dest_compression {
            CompressionAlgorithm::Brotli => {
                run_codec(dest_file_data, BrotliDecoder::new).await.unwrap()
            }
            _ => panic!("Unsupported compression for test verification"),
        };

        assert_eq!(dest_data, source_data_to_assert);
    }

    #[tokio::test]
    async fn test_copy_lambda_source_not_found() {
        let path = td_common::tdtestdir!();
        let source_store = Box::new(LocalFileSystem::new());
        let dest_store = Box::new(LocalFileSystem::new());
        let source_path = path.join("non_existent_source.txt");
        let dest_path = path.join("dest.txt");

        let source_url: Url = Url::from_file_path(&source_path).unwrap();
        let dest_url: Url = Url::from_file_path(&dest_path).unwrap();

        let source_loc = Location::LocalFile { url: source_url };
        let dest_loc = Location::LocalFile { url: dest_url };

        let lambda = copy_lambda(
            1,
            source_loc,
            source_store,
            Path::from_absolute_path(&source_path).unwrap(),
            None,
            dest_loc,
            dest_store,
            Path::from_absolute_path(&dest_path).unwrap(),
            None,
        );

        let result = lambda.await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            TransporterError::CouldNotCompressionCopyFile(e) => match e {
                CompressionCopyError::ObjectStoreError(object_store::Error::NotFound {
                    ..
                }) => {}
                _ => panic!("Wrong CompressionCopyError variant"),
            },
            _ => panic!("Wrong TransporterError variant"),
        }
    }
}
