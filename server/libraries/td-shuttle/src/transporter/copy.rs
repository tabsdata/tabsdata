//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::api::{CopyReport, CopyRequest, FileCopyReport};
use crate::transporter::common::create_store;
use crate::transporter::compression::CompressionAlgorithm;
use crate::transporter::error::TransporterError;
use crate::transporter::taskrunner::{AsyncLambda, run_tasks};
use crate::transporter::{copy_compression, copy_raw_parallel};
use std::num::NonZeroUsize;
use tracing::debug;

async fn copy_tasks_generator(
    request: &CopyRequest,
) -> Result<Vec<AsyncLambda<FileCopyReport, TransporterError>>, TransporterError> {
    let source_compression = request.decompression.map(CompressionAlgorithm::from);
    let target_compression = request.compression.map(CompressionAlgorithm::from);

    let copy_pairs = request
        .source_target_pairs()
        .iter()
        .enumerate()
        .map(|(idx, (source, target))| (idx, source, target))
        .collect::<Vec<_>>();

    let mut copy_tasks = Vec::with_capacity(copy_pairs.len());
    for (idx, source, target) in copy_pairs {
        let (source_store, source_path) = create_store(source)?;
        let source_meta = source_store.head(&source_path).await.map_err(|err| {
            TransporterError::CouldNotGetFileMetadata(source.url().to_string(), Box::new(err))
        })?;
        let (target_store, target_path) = create_store(target)?;
        let copy_task = if source_compression.is_none() && target_compression.is_none() {
            copy_raw_parallel::copy_lambda(
                idx,
                source.clone(),
                source_store,
                source_path,
                source_meta,
                target.clone(),
                target_store,
                target_path,
            )
        } else {
            copy_compression::copy_lambda(
                idx,
                source.clone(),
                source_store,
                source_path,
                source_compression,
                target.clone(),
                target_store,
                target_path,
                target_compression,
            )
        };
        copy_tasks.push(copy_task);
    }
    Ok(copy_tasks)
}

pub async fn copy(request: CopyRequest) -> Result<CopyReport, TransporterError> {
    let parallelism = NonZeroUsize::new(request.parallelism().unwrap_or(3).max(1)).unwrap();
    debug!(
        "Starting copy of {} with parallelism of {}",
        request.source_target_pairs().len(),
        parallelism
    );

    let req = copy_tasks_generator(&request).await?;
    match run_tasks(parallelism, req).await {
        Ok(reports) => {
            debug!("Finished copy");
            Ok(CopyReport::new(reports))
        }
        Err(err) => {
            debug!("Copy failed: {:?}", err);
            Err(TransporterError::CouldNotCreateImportInstructions(format!(
                "Copy failed: {:?}",
                err
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{CompressionAlgorithmSpec, CopyRequest, CopyType, Location};
    use std::fs::File;
    use std::io::{Read, Write};
    use testdir::testdir;
    use url::Url;

    #[tokio::test]
    async fn test_copy_tasks_generator_raw() {
        let dir = testdir!();
        let source_file = dir.join("source.txt");
        File::create(&source_file)
            .unwrap()
            .write_all(b"hello")
            .unwrap();
        let target_file = dir.join("target.txt");

        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };

        let request = CopyRequest::new(vec![(source.clone(), target.clone())], None, None, None);

        let tasks = super::copy_tasks_generator(&request).await.unwrap();

        assert_eq!(tasks.len(), 1);

        let report = tasks.into_iter().next().unwrap().await.unwrap();
        assert_eq!(report.copy_type, CopyType::RawParallel);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, 5);

        let output = std::fs::read_to_string(&target_file).unwrap();
        assert_eq!(output, "hello");
    }

    #[tokio::test]
    async fn test_copy_tasks_generator_compression() {
        let dir = testdir!();
        let source_file = dir.join("source.txt");
        File::create(&source_file)
            .unwrap()
            .write_all(b"hello")
            .unwrap();
        let target_file = dir.join("target.txt.gz");

        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };

        let request = CopyRequest::new(
            vec![(source.clone(), target.clone())],
            None,                                 // source decompression
            Some(CompressionAlgorithmSpec::Gzip), // target compression
            None,
        );

        let tasks = super::copy_tasks_generator(&request).await.unwrap();

        assert_eq!(tasks.len(), 1);

        let report = tasks.into_iter().next().unwrap().await.unwrap();
        assert_eq!(report.copy_type, CopyType::Compression);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, 5);

        // Check if the file is actually compressed
        let output = std::fs::read(&target_file).unwrap();
        // Gzip header is 1f 8b
        assert_eq!(&output[0..2], &[0x1f, 0x8b]);
    }

    #[tokio::test]
    async fn test_copy_tasks_generator_decompression() {
        let dir = testdir!();
        let source_file = dir.join("source.txt.gz");

        // Create a compressed source file
        let content = b"hello";
        let mut encoder = libflate::gzip::Encoder::new(Vec::new()).unwrap();
        encoder.write_all(content).unwrap();
        let compressed_content = encoder.finish().into_result().unwrap();
        File::create(&source_file)
            .unwrap()
            .write_all(&compressed_content)
            .unwrap();

        let target_file = dir.join("target.txt");

        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };

        let request = CopyRequest::new(
            vec![(source.clone(), target.clone())],
            Some(CompressionAlgorithmSpec::Gzip), // source decompression
            None,                                 // target compression
            None,
        );

        let tasks = super::copy_tasks_generator(&request).await.unwrap();

        assert_eq!(tasks.len(), 1);

        let report = tasks.into_iter().next().unwrap().await.unwrap();
        assert_eq!(report.copy_type, CopyType::Compression);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, content.len() as u64);

        let output = std::fs::read_to_string(&target_file).unwrap();
        assert_eq!(output, "hello");
    }

    #[tokio::test]
    async fn test_copy_tasks_generator_decompress_compress() {
        let dir = testdir!();
        let source_file = dir.join("source.txt.gz");

        // Create a compressed source file
        let content = b"hello";
        let mut encoder = libflate::gzip::Encoder::new(Vec::new()).unwrap();
        encoder.write_all(content).unwrap();
        let compressed_content = encoder.finish().into_result().unwrap();
        File::create(&source_file)
            .unwrap()
            .write_all(&compressed_content)
            .unwrap();

        let target_file = dir.join("target.txt.gz");

        let source = Location::LocalFile {
            url: Url::from_file_path(&source_file).unwrap(),
        };
        let target = Location::LocalFile {
            url: Url::from_file_path(&target_file).unwrap(),
        };

        let request = CopyRequest::new(
            vec![(source.clone(), target.clone())],
            Some(CompressionAlgorithmSpec::Gzip), // source decompression
            Some(CompressionAlgorithmSpec::Gzip), // target compression
            None,
        );

        let tasks = super::copy_tasks_generator(&request).await.unwrap();

        assert_eq!(tasks.len(), 1);

        let report = tasks.into_iter().next().unwrap().await.unwrap();
        assert_eq!(report.copy_type, CopyType::Compression);
        assert_eq!(report.from, source.url());
        assert_eq!(report.to, target.url());
        assert_eq!(report.size, content.len() as u64);

        // Check if the file is actually compressed
        let output = std::fs::read(&target_file).unwrap();
        // Gzip header is 1f 8b                                                                                                                                                      │
        assert_eq!(&output[0..2], &[0x1f, 0x8b]);

        // and check content
        let mut decoder = libflate::gzip::Decoder::new(&output[..]).unwrap();
        let mut decoded_content = Vec::new();
        decoder.read_to_end(&mut decoded_content).unwrap();
        assert_eq!(decoded_content, content);
    }
}
