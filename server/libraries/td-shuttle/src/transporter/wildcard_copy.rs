//
// Copyright 2025 Tabs Data Inc.
//

use crate::transporter::api::{
    AsUrl, CompressionAlgorithmSpec, CopyReport, CopyRequest, LastModifiedInfoState, Location,
    WildcardCopyRequest,
};
use crate::transporter::compression::CompressionAlgorithm;
use crate::transporter::copy::copy;
use crate::transporter::error::TransporterError;
use crate::transporter::import::files_finder;
use url::Url;

fn create_copy_source_target(
    base_source_location: &Location<Url>,
    decompression: &Option<CompressionAlgorithm>,
    base_target_location: &Location<Url>,
    compression: &Option<CompressionAlgorithm>,
    file: &Url,
) -> (Location<Url>, Location<Url>) {
    let source_file_name = file.file_name().unwrap();
    let file_name = decompression
        .map(|algo| algo.remove_compression_extension(&source_file_name))
        .unwrap_or(&source_file_name)
        .to_string();

    let file_name = match compression {
        Some(CompressionAlgorithm::Auto) => {
            panic!("Compression algorithm for wildcard copy cannot be Auto")
        }
        None | Some(CompressionAlgorithm::None) => file_name,
        Some(algo) => format!("{}.{}", file_name, algo.extension()),
    };

    let source = base_source_location.set_file(&source_file_name).unwrap();
    let target = base_target_location.set_file(&file_name).unwrap();
    (source, target)
}

fn create_copy_request(request: &WildcardCopyRequest, found: &[Url]) -> CopyRequest {
    let base_source_location: Location<Url> = request.source().location().into();
    let base_target_location: Location<Url> = request.target().into();
    let target_locations = found
        .iter()
        .map(|target| {
            create_copy_source_target(
                &base_source_location,
                &request.decompression().map(CompressionAlgorithm::from),
                &base_target_location,
                &request.compression().map(CompressionAlgorithm::from),
                target,
            )
        })
        .collect::<Vec<_>>();
    CopyRequest::new(
        target_locations,
        *request.decompression(),
        *request.compression(),
        *request.parallelism(),
    )
}

/// Copy files based on the provided wildcard copy request.
pub async fn wildcard_copy(request: WildcardCopyRequest) -> Result<CopyReport, TransporterError> {
    if request.compression() == &Some(CompressionAlgorithmSpec::Auto) {
        panic!("Compression algorithm for wildcard copy cannot be Auto")
    }

    let location = request.source().location();

    tracing::info!("Starting transporter wildcard copy for: {}", location);
    tracing::trace!("Transporter wildcard copy request:\n {:#?}", request);

    tracing::debug!(
        "Initial last modified: {:?}",
        request.source().initial_lastmod()
    );
    let received_lastmod_info = request.source().lastmod_info()?;
    tracing::trace!("Received last modified state: {:?}", received_lastmod_info);

    let lastmod_info = match received_lastmod_info {
        Some(info) => Some(info),
        None => match request.source().initial_lastmod() {
            Some(initial_lastmod) => {
                let lastmod_info = LastModifiedInfoState::new(*initial_lastmod);
                tracing::debug!("Initializing last modified state:\n {:#?}", lastmod_info);
                Some(lastmod_info)
            }
            None => None,
        },
    };

    let mut finder = files_finder(location, lastmod_info);

    let files_to_copy = finder.find_files().await?;
    let files_to_copy = files_to_copy
        .into_iter()
        .map(|(url, _)| url)
        .collect::<Vec<_>>();

    tracing::info!("Found {} files to copy", files_to_copy.len());
    tracing::trace!("Will copy:\n {:#?}", files_to_copy);

    let updated_lastmod_info = finder.lastmod_info();
    tracing::trace!(
        "New last modified information:\n {:#?}",
        updated_lastmod_info
    );

    let copy_request = create_copy_request(&request, &files_to_copy);

    tracing::debug!("Starting files wildcard copy for: {}", location);

    let report = copy(copy_request).await?;

    tracing::trace!("Transporter wildcard copy report:\n {:#?}", report);

    Ok(report)
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{
        BaseUrl, CompressionAlgorithmSpec, Location, WildcardCopyRequestBuilder,
        WildcardSourceBuilder, WildcardUrl,
    };
    use crate::transporter::wildcard_copy::wildcard_copy;
    use std::fs::File;
    use std::thread::sleep;
    use url::Url;

    #[test]
    fn test_create_copy_source_target() {
        let base_source = Location::LocalFile {
            url: Url::parse("file:///source/dir/").unwrap(),
        };
        let base_target = Location::LocalFile {
            url: Url::parse("file:///target/dir/").unwrap(),
        };
        let file = Url::parse("file:///source/dir/file1.csv").unwrap();

        let (source, target) =
            super::create_copy_source_target(&base_source, &None, &base_target, &None, &file);

        assert_eq!(
            source.url(),
            Url::parse("file:///source/dir/file1.csv").unwrap()
        );
        assert_eq!(
            target.url(),
            Url::parse("file:///target/dir/file1.csv").unwrap()
        );
    }

    #[test]
    fn test_create_copy_request() {
        let base_source = Location::LocalFile {
            url: Url::parse("file:///source/dir/").unwrap(),
        };
        let base_target = Location::LocalFile {
            url: Url::parse("file:///target/dir/").unwrap(),
        };
        let request = WildcardCopyRequestBuilder::default()
            .source(
                WildcardSourceBuilder::default()
                    .location::<Location<WildcardUrl>>((&base_source).into())
                    .initial_lastmod(None)
                    .lastmod_info(None)
                    .build()
                    .unwrap(),
            )
            .target::<Location<BaseUrl>>((&base_target).into())
            .parallelism(None)
            .build()
            .unwrap();

        let found = vec![
            Url::parse("file:///source/dir/file1.csv").unwrap(),
            Url::parse("file:///source/dir/file2.csv").unwrap(),
        ];

        let copy_request = super::create_copy_request(&request, &found);

        assert_eq!(copy_request.source_target_pairs().len(), 2);
        assert_eq!(
            copy_request.source_target_pairs()[0].0.url(),
            Url::parse("file:///source/dir/file1.csv").unwrap()
        );
        assert_eq!(
            copy_request.source_target_pairs()[0].1.url(),
            Url::parse("file:///target/dir/file1.csv").unwrap()
        );
        assert_eq!(
            copy_request.source_target_pairs()[1].0.url(),
            Url::parse("file:///source/dir/file2.csv").unwrap()
        );
        assert_eq!(
            copy_request.source_target_pairs()[1].1.url(),
            Url::parse("file:///target/dir/file2.csv").unwrap()
        );
    }

    //TODO do test the crap
    #[tokio::test]
    async fn test_find_and_copy() {
        let dir = testdir!();
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let target_dir = dir.join("target");
        std::fs::create_dir_all(&target_dir).unwrap();
        let f1 = File::create(source_dir.join("file1.csv")).unwrap();
        sleep(std::time::Duration::from_millis(2));
        let _f2 = File::create(source_dir.join("file1.csv")).unwrap();
        let wildcard_path = source_dir.join("file*.csv");
        let source_url = Url::from_file_path(&wildcard_path).unwrap();
        let target_url = Url::from_file_path(&target_dir).unwrap();

        let source = WildcardSourceBuilder::default()
            .location(Location::LocalFile {
                url: WildcardUrl(source_url.clone()),
            })
            .initial_lastmod(Some(f1.metadata().unwrap().modified().unwrap().into()))
            .lastmod_info(None)
            .build()
            .unwrap();
        let target = Location::LocalFile {
            url: BaseUrl(target_url.clone()),
        };
        let request = WildcardCopyRequestBuilder::default()
            .source(source)
            .target(target)
            .parallelism(None)
            .build()
            .unwrap();

        let report = wildcard_copy(request).await.unwrap();
        assert_eq!(report.files().len(), 1);
        let file_report = &report.files()[0];
        assert_eq!(file_report.to, target_url.join("file1.csv").unwrap());
    }

    use crate::transporter::compression::CompressionAlgorithm;
    use async_compression::tokio::bufread::{
        BrotliDecoder, BrotliEncoder, BzDecoder, BzEncoder, GzipDecoder, GzipEncoder, Lz4Decoder,
        Lz4Encoder, XzDecoder, XzEncoder, ZlibDecoder, ZlibEncoder, ZstdDecoder, ZstdEncoder,
    };
    use std::fs;
    use std::io::Cursor;
    use testdir::testdir;
    use tokio::io::{AsyncReadExt, BufReader};

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

    async fn test_wildcard_copy(
        source_compression_spec: Option<CompressionAlgorithmSpec>,
        source_ext: &str,
        dest_compression_spec: Option<CompressionAlgorithmSpec>,
    ) {
        let dir = testdir!();
        let source_dir = dir.join("source");
        fs::create_dir_all(&source_dir).unwrap();
        let target_dir = dir.join("target/");
        fs::create_dir_all(&target_dir).unwrap();

        let source_data =
            b"hello worldhello worldhello worldhello worldhello worldhello worldhello world"
                .to_vec();

        let files_to_create = vec!["file1", "file2"];

        let source_compression: Option<CompressionAlgorithm> =
            source_compression_spec.map(CompressionAlgorithm::from);
        let dest_compression: Option<CompressionAlgorithm> =
            dest_compression_spec.map(CompressionAlgorithm::from);

        for file_base_name in &files_to_create {
            let mut file_name_with_ext = format!("{}.{}", file_base_name, source_ext);
            if let Some(compression) = &source_compression {
                file_name_with_ext = format!("{}.{}", file_name_with_ext, compression.extension());
            }
            let source_path = source_dir.join(&file_name_with_ext);

            let resolved_source_compression = CompressionAlgorithm::resolve_algorithm(
                &source_compression,
                source_path.to_str().unwrap(),
            );

            let source_file_data: Vec<u8> = match resolved_source_compression {
                CompressionAlgorithm::None => source_data.clone(),
                CompressionAlgorithm::Brotli => run_codec(source_data.clone(), BrotliEncoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Bzip => run_codec(source_data.clone(), BzEncoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Gzip => run_codec(source_data.clone(), GzipEncoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Lz4 => run_codec(source_data.clone(), Lz4Encoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Xz => run_codec(source_data.clone(), XzEncoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Zlib => run_codec(source_data.clone(), ZlibEncoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Zstd => run_codec(source_data.clone(), ZstdEncoder::new)
                    .await
                    .unwrap(),
                CompressionAlgorithm::Auto => {
                    panic!("Source compression should not be Auto inside test_wildcard_copy");
                }
            };
            fs::write(&source_path, source_file_data).unwrap();
            sleep(std::time::Duration::from_millis(2));
        }

        let wildcard_path = source_dir.join(format!("*.{}*", source_ext));
        let source_url = Url::from_file_path(&wildcard_path).unwrap();
        let target_url = Url::from_directory_path(&target_dir).unwrap();

        let source = WildcardSourceBuilder::default()
            .location(Location::LocalFile {
                url: WildcardUrl(source_url.clone()),
            })
            .initial_lastmod(None)
            .lastmod_info(None)
            .build()
            .unwrap();
        let target = Location::LocalFile {
            url: BaseUrl(target_url.clone()),
        };
        let request = WildcardCopyRequestBuilder::default()
            .source(source)
            .target(target)
            .decompression(source_compression_spec)
            .compression(dest_compression_spec)
            .parallelism(None)
            .build()
            .unwrap();

        let report = wildcard_copy(request).await.unwrap();
        assert_eq!(report.files().len(), files_to_create.len());

        for file_base_name in files_to_create {
            let file_name_with_ext = format!("{}.{}", file_base_name, source_ext);

            let dest_compression = dest_compression.unwrap_or(CompressionAlgorithm::None);

            let dest_path = if dest_compression == CompressionAlgorithm::None {
                target_dir.join(file_name_with_ext)
            } else {
                target_dir.join(format!(
                    "{}.{}",
                    file_name_with_ext,
                    dest_compression.extension()
                ))
            };

            let dest_file_data = fs::read(&dest_path).unwrap();

            let dest_data: Vec<u8> = match dest_compression {
                CompressionAlgorithm::None => dest_file_data,
                CompressionAlgorithm::Brotli => {
                    run_codec(dest_file_data, BrotliDecoder::new).await.unwrap()
                }
                CompressionAlgorithm::Bzip => {
                    run_codec(dest_file_data, BzDecoder::new).await.unwrap()
                }
                CompressionAlgorithm::Gzip => {
                    run_codec(dest_file_data, GzipDecoder::new).await.unwrap()
                }
                CompressionAlgorithm::Lz4 => {
                    run_codec(dest_file_data, Lz4Decoder::new).await.unwrap()
                }
                CompressionAlgorithm::Xz => {
                    run_codec(dest_file_data, XzDecoder::new).await.unwrap()
                }
                CompressionAlgorithm::Zlib => {
                    run_codec(dest_file_data, ZlibDecoder::new).await.unwrap()
                }
                CompressionAlgorithm::Zstd => {
                    run_codec(dest_file_data, ZstdDecoder::new).await.unwrap()
                }
                CompressionAlgorithm::Auto => {
                    // This can happen if dest_compression is Auto and extension is unknown
                    // For this test, we assume it resolves to None if not a known ext.
                    dest_file_data
                }
            };

            assert_eq!(dest_data, source_data);
        }
    }

    #[tokio::test]
    async fn test_wildcard_copy_none_to_none() {
        test_wildcard_copy(None, "txt", None).await;
    }

    #[tokio::test]
    async fn test_wildcard_copy_gzip_to_none() {
        test_wildcard_copy(Some(CompressionAlgorithmSpec::Gzip), "txt", None).await;
    }

    #[tokio::test]
    async fn test_wildcard_copy_none_to_gzip() {
        test_wildcard_copy(None, "txt", Some(CompressionAlgorithmSpec::Gzip)).await;
    }

    #[tokio::test]
    async fn test_wildcard_copy_brotli_to_zstd() {
        test_wildcard_copy(
            Some(CompressionAlgorithmSpec::Brotli),
            "txt",
            Some(CompressionAlgorithmSpec::Zstd),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_wildcard_copy_auto_to_auto() {
        test_wildcard_copy(
            Some(CompressionAlgorithmSpec::Auto),
            "gz",
            Some(CompressionAlgorithmSpec::Auto),
        )
        .await;
    }
}
