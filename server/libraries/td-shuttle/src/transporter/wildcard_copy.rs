//
// Copyright 2025 Tabs Data Inc.
//

use crate::transporter::api::{
    AsUrl, CopyReport, CopyRequest, LastModifiedInfoState, Location, WildcardCopyRequest,
};
use crate::transporter::copy::copy;
use crate::transporter::error::TransporterError;
use crate::transporter::import::files_finder;
use url::Url;

fn create_copy_source_target(
    base_source_location: &Location<Url>,
    base_target_location: &Location<Url>,
    file: &Url,
) -> (Location<Url>, Location<Url>) {
    let name = file.file_name().unwrap();
    let source = base_source_location.set_file(&name).unwrap();
    let target = base_target_location.set_file(&name).unwrap();
    (source, target)
}

fn create_copy_request(request: &WildcardCopyRequest, found: &[Url]) -> CopyRequest {
    let base_source_location: Location<Url> = request.source().location().into();
    let base_target_location: Location<Url> = request.target().into();
    let target_locations = found
        .iter()
        .map(|target| {
            create_copy_source_target(&base_source_location, &base_target_location, target)
        })
        .collect::<Vec<_>>();
    CopyRequest::new(target_locations, *request.parallelism())
}

/// Copy files based on the provided wildcard copy request.
pub async fn wildcard_copy(request: WildcardCopyRequest) -> Result<CopyReport, TransporterError> {
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
        BaseUrl, Location, WildcardCopyRequestBuilder, WildcardSourceBuilder, WildcardUrl,
    };
    use crate::transporter::wildcard_copy::wildcard_copy;
    use std::fs::File;
    use std::thread::sleep;
    use testdir::testdir;
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

        let (source, target) = super::create_copy_source_target(&base_source, &base_target, &file);

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
}
