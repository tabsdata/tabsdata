//
// Copyright 2025. Tabs Data Inc.
//

use crate::transporter::api::{CopyType, FileCopyReport, Location};
use crate::transporter::error::{TransporterError, range_to_string};
use crate::transporter::taskrunner::AsyncLambda;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, PutPayload};
use std::ops::Range;
use std::sync::Arc;
use td_common::time::UniqueUtc;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tracing::{debug, trace};
use url::Url;

#[allow(clippy::too_many_arguments)]
pub fn copy_lambda(
    idx: usize,
    source: Location<Url>,
    source_store: Box<dyn ObjectStore>,
    source_path: Path,
    source_meta: ObjectMeta,
    target: Location<Url>,
    target_store: Box<dyn ObjectStore>,
    target_path: Path,
) -> AsyncLambda<FileCopyReport, TransporterError> {
    let buffer_size = target.buffer_size();
    let (read_ahead, write_parallel) = source.read_write_ratio(&target);
    Box::pin(async move {
        let task = CopyTask::new(
            idx,
            source,
            source_store,
            source_path,
            source_meta,
            target,
            target_store,
            target_path,
            buffer_size,
            read_ahead,
            write_parallel,
        )
        .await?;
        task.copy().await
    })
}

#[derive(Debug)]
struct CopyTask {
    idx: usize,
    source: Location<Url>,
    source_store: Arc<Box<dyn ObjectStore>>,
    source_path: Path,
    target: Location<Url>,
    target_store: Arc<Box<dyn ObjectStore>>,
    target_path: Path,
    size: u64,
    ranges: Vec<(Range<u64>, bool)>,
    read_ahead: usize,
    write_parallel: usize,
}

#[derive(Debug)]
pub struct Message {
    range: Range<u64>,
    data: Bytes,
    last: bool,
}

impl CopyTask {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        idx: usize,
        source: Location<Url>,
        source_store: Box<dyn ObjectStore>,
        source_path: Path,
        source_meta: ObjectMeta,
        target: Location<Url>,
        target_store: Box<dyn ObjectStore>,
        target_path: Path,
        buffer_size: usize,
        read_ahead: usize,
        write_parallel: usize,
    ) -> Result<Self, TransporterError> {
        let buffer_size_u64 = buffer_size as u64;

        let size = source_meta.size;

        // create ranges
        let ranges = if size == 0 {
            // empty range for empty file
            vec![(0..0, true)]
        } else {
            let full_ranges = size / buffer_size_u64;
            let remainder_size = size % buffer_size_u64;
            let number_of_ranges = full_ranges + if remainder_size > 0 { 1 } else { 0 };
            let mut ranges = Vec::with_capacity(number_of_ranges as usize);
            for i in 0..full_ranges {
                ranges.push((i * buffer_size_u64..(i + 1) * buffer_size_u64, false));
            }
            if remainder_size > 0 {
                ranges.push((
                    full_ranges * buffer_size_u64..full_ranges * buffer_size_u64 + remainder_size,
                    false,
                ));
            }
            // mark last range as true
            if let Some((_, last)) = ranges.last_mut() {
                *last = true;
            }
            ranges
        };

        let task = Self {
            idx,
            source,
            source_store: Arc::new(source_store),
            source_path,
            target,
            target_store: Arc::new(target_store),
            target_path,
            size,
            ranges,
            read_ahead,
            write_parallel,
        };
        Ok(task)
    }

    async fn copy(&self) -> Result<FileCopyReport, TransporterError> {
        debug!(
            "Starting copy of file {} to {}",
            self.source.url(),
            self.target.url()
        );
        let start = UniqueUtc::now_millis();
        let (sender, receiver) = channel::<Message>(self.read_ahead);
        let writer = Writer::new(
            self.target.clone(),
            self.target_store.clone(),
            self.target_path.clone(),
            self.write_parallel,
        )
        .await?;
        let writer = tokio::spawn(async move { writer.write(receiver).await });
        self.read(sender).await?;
        match writer.await {
            Ok(result) => result?,
            Err(err) => {
                return Err(TransporterError::CouldNotCopyFile(
                    self.target.url().to_string(),
                    err.to_string(),
                ));
            }
        }
        let end = UniqueUtc::now_millis();
        let report = FileCopyReport {
            copy_type: CopyType::RawParallel,
            idx: self.idx,
            from: self.source.url(),
            size: self.size,
            to: self.target.url(),
            started_at: start,
            ended_at: end,
        };
        debug!(
            "Finished copy of file {} to {}",
            self.source.url(),
            self.target.url()
        );
        Ok(report)
    }

    async fn read(&self, sender: Sender<Message>) -> Result<(), TransporterError> {
        for (range, last) in self.ranges.iter() {
            trace!(
                "Reading {} range {}",
                self.source.url(),
                range_to_string(range)
            );
            eprintln!(
                "Reading {} range {}",
                self.source.url(),
                range_to_string(range)
            );
            // Since this commit:
            // https://github.com/apache/arrow-rs-object-store/commit/6c3de451a7931bff1c561632c31e3cb9f01faf8b
            // (Fix LocalFileSystem with range request that ends beyond end of file)
            // using 0 for start and end raises an error. We just send and empty Bytes in this case.
            let data = if range.start == 0 && range.end == 0 {
                Bytes::new()
            } else {
                self.source_store
                    .get_range(&self.source_path, range.clone())
                    .await
                    .map_err(|err| {
                        TransporterError::CouldNotGetFileRange(
                            self.source_path.to_string(),
                            range_to_string(range),
                            Box::new(err),
                        )
                    })?
            };
            sender
                .send(Message {
                    range: range.clone(),
                    data,
                    last: *last,
                })
                .await
                .map_err(|err| {
                    TransporterError::CouldNotSendBlock(
                        self.source_path.to_string(),
                        range_to_string(range),
                        err,
                    )
                })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Writer {
    target: Location<Url>,
    target_store: Arc<Box<dyn ObjectStore>>,
    target_path: Path,
    write_parallel: usize,
}

impl Writer {
    async fn new(
        target: Location<Url>,
        target_store: Arc<Box<dyn ObjectStore>>,
        target_path: Path,
        write_parallel: usize,
    ) -> Result<Self, TransporterError> {
        Ok(Self {
            target,
            target_store,
            target_path,
            write_parallel,
        })
    }

    async fn write(&self, mut receiver: Receiver<Message>) -> Result<(), TransporterError> {
        let mut multipart_upload = self
            .target_store
            .put_multipart(&self.target_path)
            .await
            .map_err(|err| {
                TransporterError::CouldNotCreateMultipart(
                    self.target_path.to_string(),
                    Box::new(err),
                )
            })?;
        let mut blocks_writing = FuturesOrdered::new();
        loop {
            while blocks_writing.len() >= self.write_parallel {
                // Limit concurrent writes locking if passed parallelism
                let _ = blocks_writing.next().await.unwrap();
            }
            if let Some(message) = receiver.recv().await {
                trace!(
                    "Writing {} range {}",
                    self.target.url(),
                    range_to_string(&message.range)
                );
                let part = multipart_upload.put_part(PutPayload::from(message.data));
                blocks_writing.push_back(part);
                if message.last {
                    break;
                }
            }
        }

        // waits until all writes are done
        while blocks_writing.next().await.is_some() {}

        trace!("Completing writing {}", self.target.url());
        multipart_upload.complete().await.map_err(|err| {
            TransporterError::CouldNotCompleteMultipartUpload(
                self.target_path.to_string(),
                Box::new(err),
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::Location;
    use crate::transporter::common::create_store;
    use crate::transporter::copy_raw_parallel::{CopyTask, copy_lambda};
    use crate::transporter::error::TransporterError;
    use std::fs::File;
    use std::io::Write;
    use td_common::time::UniqueUtc;
    use testdir::testdir;
    use url::Url;

    async fn test_copy_task(input: &str) {
        let dir = testdir!();
        let source_file = dir.join("source.txt");
        File::create(&source_file)
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let target_file = dir.join("target.txt");
        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };
        let before = UniqueUtc::now_millis();

        let (source_store, source_path) = create_store(&source).unwrap();
        let source_meta = source_store
            .head(&source_path)
            .await
            .map_err(|err| {
                TransporterError::CouldNotGetFileMetadata(source.url().to_string(), Box::new(err))
            })
            .unwrap();
        let (target_store, target_path) = create_store(&target).unwrap();

        let task = CopyTask::new(
            0,
            source.clone(),
            source_store,
            source_path,
            source_meta,
            target.clone(),
            target_store,
            target_path,
            2,
            2,
            2,
        )
        .await
        .unwrap();
        let report = task.copy().await.unwrap();
        let after = UniqueUtc::now_millis();
        assert_eq!(report.idx, 0);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, input.len() as u64);
        assert!(report.started_at > before);
        assert!(report.ended_at < after);
        assert!(report.ended_at > report.started_at);
        let output = std::fs::read_to_string(&target_file).unwrap();
        assert_eq!(output, input);
    }

    #[tokio::test]
    async fn test_copy_task_input() {
        test_copy_task("Hello, World!").await;
    }

    #[tokio::test]
    async fn test_copy_task_empty_input() {
        test_copy_task("").await;
    }

    async fn test_copy_lambda(input: &str) {
        let dir = testdir!();
        let source_file = dir.join("source.txt");
        File::create(&source_file)
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let target_file = dir.join("target.txt");
        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };
        let before = UniqueUtc::now_millis();

        let (source_store, source_path) = create_store(&source).unwrap();
        let source_meta = source_store
            .head(&source_path)
            .await
            .map_err(|err| {
                TransporterError::CouldNotGetFileMetadata(source.url().to_string(), Box::new(err))
            })
            .unwrap();
        let (target_store, target_path) = create_store(&target).unwrap();

        let lambda = copy_lambda(
            0,
            source.clone(),
            source_store,
            source_path,
            source_meta,
            target.clone(),
            target_store,
            target_path,
        );

        let report = lambda.await.unwrap();
        let after = UniqueUtc::now_millis();
        assert_eq!(report.idx, 0);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, input.len() as u64);
        assert!(report.started_at > before);
        assert!(report.ended_at < after);
        assert!(report.ended_at > report.started_at);
        let output = std::fs::read_to_string(&target_file).unwrap();
        assert_eq!(output, input);
    }

    #[tokio::test]
    async fn test_copy_lambda_input() {
        let twenty_mb_string = "a".repeat(20 * 1024 * 1024);
        test_copy_lambda(&twenty_mb_string).await;
    }

    #[tokio::test]
    async fn test_copy_lambda_empty_input() {
        test_copy_lambda("").await;
    }
}
