//
// Copyright 2024 Tabs Data Inc.
//

use crate::bin::transporter::api::{CopyReport, CopyRequest, FileCopyReport, Location};
use crate::bin::transporter::common::create_store;
use crate::bin::transporter::error::{range_to_string, TransporterError};
use bytes::Bytes;
use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, PutPayload};
use std::ops::Range;
use td_common::time::UniqueUtc;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::{debug, trace};
use url::Url;

pub async fn copy(request: CopyRequest) -> Result<CopyReport, TransporterError> {
    let parallelism = request.parallelism().unwrap_or(3);
    debug!(
        "Starting copy of {} with parallelism of {}",
        request.source_target_pairs().len(),
        parallelism
    );
    let mut reports = Vec::with_capacity(request.source_target_pairs().len());
    for (idx, (source, target)) in request.source_target_pairs().iter().enumerate() {
        let task = CopyTask::new(
            idx,
            source.clone(),
            target.clone(),
            target.buffer_size(),
            parallelism,
        )
        .await?;
        reports.push(task.copy().await?);
    }
    debug!("Finished copy");
    Ok(CopyReport::new(reports))
}

#[derive(Debug)]
struct CopyTask {
    idx: usize,
    source: Location<Url>,
    source_store: Box<dyn ObjectStore>,
    source_path: Path,
    target: Location<Url>,
    size: usize,
    ranges: Vec<(Range<usize>, bool)>,
    parallelism: usize,
}

#[derive(Debug)]
pub struct Message {
    range: Range<usize>,
    data: Bytes,
    last: bool,
}

impl CopyTask {
    pub async fn new(
        idx: usize,
        source: Location<Url>,
        target: Location<Url>,
        buffer_size: usize,
        parallelism: usize,
    ) -> Result<Self, TransporterError> {
        let (source_store, source_path) = create_store(&source)?;

        let source_meta = source_store.head(&source_path).await.map_err(|err| {
            TransporterError::CouldNotGetFileMetadata(source.url().to_string(), Box::new(err))
        })?;
        let size = source_meta.size;

        // create ranges
        let ranges = if size == 0 {
            // empty range for empty file
            vec![(0..0, true)]
        } else {
            let full_ranges = size / buffer_size;
            let remainder_size = size % buffer_size;
            let number_of_ranges = full_ranges + if remainder_size > 0 { 1 } else { 0 };
            let mut ranges = Vec::with_capacity(number_of_ranges);
            for i in 0..full_ranges {
                ranges.push((i * buffer_size..(i + 1) * buffer_size, false));
            }
            if remainder_size > 0 {
                ranges.push((
                    full_ranges * buffer_size..full_ranges * buffer_size + remainder_size,
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
            source_store,
            source_path,
            target,
            size,
            ranges,
            parallelism,
        };
        Ok(task)
    }

    async fn copy(&self) -> Result<FileCopyReport, TransporterError> {
        debug!(
            "Starting copy of file {} to {}",
            self.source.url(),
            self.target.url()
        );
        let start = UniqueUtc::now_millis().await;
        let (sender, receiver) = channel::<Message>(self.parallelism);
        let writer = Writer::new(self.target.clone(), self.parallelism).await?;
        let writer = tokio::spawn(async move { writer.write(receiver).await });
        self.read(sender).await?;
        let _ = writer.await.unwrap();
        let end = UniqueUtc::now_millis().await;
        let report = FileCopyReport {
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
            let data = self
                .source_store
                .get_range(&self.source_path, range.clone())
                .await
                .map_err(|err| {
                    TransporterError::CouldNotGetFileRange(
                        self.source_path.to_string(),
                        range_to_string(range),
                        Box::new(err),
                    )
                })?;
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
    target_store: Box<dyn ObjectStore>,
    target_path: Path,
    parallelism: usize,
}

impl Writer {
    pub async fn new(target: Location<Url>, parallelism: usize) -> Result<Self, TransporterError> {
        let (target_store, target_path) = create_store(&target)?;
        Ok(Self {
            target,
            target_store,
            target_path,
            parallelism,
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
            while blocks_writing.len() >= self.parallelism {
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
    use crate::bin::transporter::api::Location;
    use crate::bin::transporter::copy::CopyTask;
    use std::fs::File;
    use std::io::Write;
    use td_common::absolute_path::AbsolutePath;
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
        let before = UniqueUtc::now_millis().await;
        let task = CopyTask::new(0, source.clone(), target.clone(), 2, 2)
            .await
            .unwrap();
        let report = task.copy().await.unwrap();
        let after = UniqueUtc::now_millis().await;
        assert_eq!(report.idx, 0);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, input.len());
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

    fn create_source_target(name: &str) -> (Location<Url>, Location<Url>, Vec<u8>) {
        let data = name.repeat(10).as_bytes().to_vec();
        let dir = testdir!();
        let source_file = dir.join(format!("input-{}", name));
        File::create(&source_file)
            .unwrap()
            .write_all(data.as_slice())
            .unwrap();
        let target_file = dir.join(format!("output-{}", name));
        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };
        (source, target, data)
    }

    #[tokio::test]
    async fn test_copy() {
        let (source0, target0, input0) = create_source_target("data0");
        let (source1, target1, input1) = create_source_target("data1");
        let request = super::CopyRequest::new(vec![(source0, target0), (source1, target1)], None);
        let report = super::copy(request).await.unwrap();
        assert_eq!(report.files().len(), 2);
        assert_eq!(report.files()[0].idx, 0);
        assert_eq!(report.files()[1].idx, 1);
        let output0 = std::fs::read_to_string(report.files()[0].to.abs_path()).unwrap();
        assert_eq!(output0.as_bytes(), input0);
        let output1 = std::fs::read_to_string(report.files()[1].to.abs_path()).unwrap();
        assert_eq!(output1.as_bytes(), input1);
    }
}
