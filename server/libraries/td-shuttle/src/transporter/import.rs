//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::api::{
    AsUrl, ImportReport, ImportRequest, LastModifiedInfo, LastModifiedInfoState, Location,
    WildcardUrl,
};
use crate::transporter::common::create_store;
use crate::transporter::error::TransporterError;
use crate::transporter::files_importer::{FilesImporter, Importer};
use async_trait::async_trait;
use object_store::Error::NotFound;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use url::Url;
use wildmatch::WildMatch;

/// A trait that defines the methods for finding files to import.
#[async_trait]
trait FileFinder {
    /// Returns the URL from which files will be imported. It may contain wildcards in the file name only.
    fn import_url(&self) -> Url;

    /// Finds the [`ObjectMeta`] for the files to import.
    async fn find(&mut self) -> Result<Vec<ObjectMeta>, TransporterError>;

    /// Finds the files to import and returns their [`ObjectStore`] paths.
    async fn find_files(&mut self) -> Result<Vec<(Url, ObjectMeta)>, TransporterError> {
        let mut base_url = self.import_url();
        base_url
            .path_segments_mut()
            .map_err(|_| TransporterError::InvalidImporterFileUrl(self.import_url().to_string()))?
            .pop();

        let to_url = |path: &Path| -> Url {
            let mut url = base_url.clone();
            url.path_segments_mut().unwrap().extend(path.filename());
            url
        };

        self.find().await.map(|oms| {
            oms.into_iter()
                .map(|m| (to_url(&m.location), m))
                .collect::<Vec<_>>()
        })
    }

    /// Returns up to date last modified information based on files found by the [`find_files`] method
    /// invocations (typically one invocation).
    fn lastmod_info(&self) -> Option<LastModifiedInfoState>;
}

/// A finder that locates files to import from a single file URL without last modified check.
struct SingleFileFinder {
    location: Location<WildcardUrl>,
}

impl SingleFileFinder {
    pub fn new(location: Location<WildcardUrl>) -> Self {
        Self { location }
    }
}

#[async_trait]
impl FileFinder for SingleFileFinder {
    fn import_url(&self) -> Url {
        self.location.url()
    }

    async fn find(&mut self) -> Result<Vec<ObjectMeta>, TransporterError> {
        let (store, _) = create_store(&self.location)?;
        let path = Path::parse(self.location.url().path()).map_err(|_| {
            TransporterError::InvalidImporterFileUrl(self.location.url().to_string())
        })?;
        match store.head(&path).await {
            Ok(head) => Ok(vec![head]),
            Err(NotFound { .. }) => Ok(Vec::new()),
            Err(err) => Err(TransporterError::CouldListFilesToImport(
                self.location.url().to_string(),
                err,
            )),
        }
    }

    fn lastmod_info(&self) -> Option<LastModifiedInfoState> {
        None
    }
}

/// A finder that locates files to import from a single file URL without last modified check.
struct SingleFileLastModFinder {
    finder: SingleFileFinder,
    last_modified_info: LastModifiedInfoState,
}

impl SingleFileLastModFinder {
    pub fn new(location: Location<WildcardUrl>, last_modified_info: LastModifiedInfoState) -> Self {
        Self {
            finder: SingleFileFinder::new(location),
            last_modified_info,
        }
    }
}

/// Finds files to import and checks if they are newer than the last modified information.
async fn find_using_last_modified<F: FileFinder>(
    finder: &mut F,
    lastmod_info: &mut LastModifiedInfoState,
) -> Result<Vec<ObjectMeta>, TransporterError> {
    let matching_files = finder.find().await?;
    let mut newer_files = Vec::with_capacity(matching_files.len());
    for file_meta in matching_files {
        let file_url = file_meta.location.to_string();
        let file_lastmod = file_meta.last_modified;
        if lastmod_info.check_and_set(finder.import_url().path(), &file_url, &file_lastmod)? {
            newer_files.push(file_meta);
        }
    }
    Ok(newer_files)
}

#[async_trait]
impl FileFinder for SingleFileLastModFinder {
    fn import_url(&self) -> Url {
        self.finder.import_url()
    }

    async fn find(&mut self) -> Result<Vec<ObjectMeta>, TransporterError> {
        find_using_last_modified(&mut self.finder, &mut self.last_modified_info).await
    }

    fn lastmod_info(&self) -> Option<LastModifiedInfoState> {
        Some(self.last_modified_info.clone())
    }
}

/// A finder that locates files to import based on a file pattern (wildcard) in the URL.
struct PatternFileFinder {
    location: Location<WildcardUrl>,
}

impl PatternFileFinder {
    pub fn new(location: Location<WildcardUrl>) -> Self {
        Self { location }
    }
}

#[async_trait]
impl FileFinder for PatternFileFinder {
    fn import_url(&self) -> Url {
        self.location.url()
    }

    async fn find(&mut self) -> Result<Vec<ObjectMeta>, TransporterError> {
        let (store, _) = create_store(&self.location)?;

        let base_path = self.location.url().base_path();
        let base_path = Path::parse(base_path).map_err(|_| {
            TransporterError::InvalidImporterFileUrl(self.location.url().to_string())
        })?;

        let file_pattern = self.location.url().file_name().ok_or_else(|| {
            TransporterError::InvalidImporterFileUrl(self.location.url().to_string())
        })?;
        let file_matcher = WildMatch::new(&file_pattern);

        let found = store
            .list_with_delimiter(Some(&base_path))
            .await
            .map_err(|err| {
                TransporterError::CouldListFilesToImport(self.location.url().to_string(), err)
            })?;

        // filter files based on the file pattern
        let mut found: Vec<ObjectMeta> = found
            .objects
            .into_iter()
            .filter(|meta| file_matcher.matches(meta.location.filename().unwrap()))
            .collect();

        // sort files by last modified time
        found.sort_by_key(|meta| meta.last_modified);

        Ok(found)
    }

    fn lastmod_info(&self) -> Option<LastModifiedInfoState> {
        None
    }
}

/// A finder that locates files to import based on a file pattern (wildcard) in the URL.
struct PatternLastModFileFinder {
    finder: PatternFileFinder,
    last_modified_info: LastModifiedInfoState,
}

impl PatternLastModFileFinder {
    pub fn new(location: Location<WildcardUrl>, last_modified_info: LastModifiedInfoState) -> Self {
        Self {
            finder: PatternFileFinder::new(location),
            last_modified_info,
        }
    }
}

#[async_trait]
impl FileFinder for PatternLastModFileFinder {
    fn import_url(&self) -> Url {
        self.finder.import_url()
    }

    async fn find(&mut self) -> Result<Vec<ObjectMeta>, TransporterError> {
        find_using_last_modified(&mut self.finder, &mut self.last_modified_info).await
    }

    fn lastmod_info(&self) -> Option<LastModifiedInfoState> {
        Some(self.last_modified_info.clone())
    }
}

fn import_files_finder(
    location: &Location<WildcardUrl>,
    last_modified_info: Option<LastModifiedInfoState>,
) -> Box<dyn FileFinder + Send + Sync> {
    let location = location.clone();
    let finder: Box<dyn FileFinder + Send + Sync> = if location.url().has_wildcard() {
        match last_modified_info {
            Some(info) => Box::new(PatternLastModFileFinder::new(location, info)),
            None => Box::new(PatternFileFinder::new(location)),
        }
    } else {
        match last_modified_info {
            Some(info) => Box::new(SingleFileLastModFinder::new(location, info)),
            None => Box::new(SingleFileFinder::new(location)),
        }
    };
    finder
}

async fn find_and_import<I: Importer>(
    request: ImportRequest,
) -> Result<ImportReport, TransporterError> {
    let location = request.source().location();

    tracing::info!("Starting transporter import for: {}", location);
    tracing::trace!("Transporter import request:\n {:#?}", request);

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

    let mut finder = import_files_finder(location, lastmod_info);

    let files_to_import = finder.find_files().await?;
    tracing::info!("Found {} files to import", files_to_import.len());
    tracing::trace!("Will import:\n {:#?}", files_to_import);

    let updated_lastmod_info = finder.lastmod_info();
    tracing::trace!(
        "New last modified information:\n {:#?}",
        updated_lastmod_info
    );

    tracing::debug!("Starting files import for: {}", location);

    let reports = I::import(&request, files_to_import).await?;

    tracing::debug!("Finishing files import for: {}", location);

    let report = ImportReport::new(reports, updated_lastmod_info)?;

    tracing::trace!("Transporter import report:\n {:#?}", report);

    Ok(report)
}

/// Imports files based on the provided import request.
pub async fn import(request: ImportRequest) -> Result<ImportReport, TransporterError> {
    find_and_import::<FilesImporter>(request).await
}

#[cfg(test)]
mod tests {
    use crate::transporter::api::{
        BaseImportUrl, FileImportReport, ImportFormat, ImportRequest, ImportRequestBuilder,
        ImportSourceBuilder, ImportTargetBuilder, LastModifiedInfoAccessor, LastModifiedInfoState,
        Location, WildcardUrl,
    };
    use crate::transporter::error::TransporterError;
    use crate::transporter::files_importer::Importer;
    use crate::transporter::import::{
        FileFinder, PatternFileFinder, PatternLastModFileFinder, SingleFileFinder,
        SingleFileLastModFinder, find_and_import,
    };
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use object_store::ObjectMeta;
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;
    use td_common::time::UniqueUtc;
    use testdir::testdir;
    use tokio::time::sleep;
    use url::Url;
    use uuid::Uuid;

    fn create_file(path: &std::path::Path) {
        let mut file = File::create(path).unwrap();
        file.write_all(b"test").unwrap();
    }

    async fn test_single_file_finder(url: Url, found: Vec<Url>) {
        let location = Location::LocalFile {
            url: WildcardUrl(url.clone()),
        };
        let mut finder = SingleFileFinder::new(location);

        assert_eq!(finder.import_url(), url);

        let files = finder
            .find_files()
            .await
            .unwrap()
            .into_iter()
            .map(|(u, _)| u)
            .collect::<Vec<_>>();
        assert_eq!(files, found);
        assert!(finder.lastmod_info().is_none());
    }

    #[tokio::test]
    async fn test_single_file_finder_not_found() {
        let dir = testdir!();
        let file = dir.join("file.csv");
        let url = Url::from_file_path(&file).unwrap();

        test_single_file_finder(url, vec![]).await;
    }

    #[tokio::test]
    async fn test_single_file_finder_found() {
        let dir = testdir!();
        let file = dir.join("file.csv");
        let url = Url::from_file_path(&file).unwrap();
        create_file(&file);

        test_single_file_finder(url.clone(), vec![url]).await;
    }

    async fn test_pattern_file_finder(url: Url, found: Vec<Url>) {
        let location = Location::LocalFile {
            url: WildcardUrl(url.clone()),
        };
        let mut finder = PatternFileFinder::new(location);

        assert_eq!(finder.import_url(), url);

        let files = finder
            .find_files()
            .await
            .unwrap()
            .into_iter()
            .map(|(u, _)| u)
            .collect::<Vec<_>>();
        assert_eq!(files, found);
        assert!(finder.lastmod_info().is_none());
    }

    #[tokio::test]
    async fn test_pattern_file_finder_not_found() {
        let dir = testdir!();
        let file = dir.join("file*.csv");
        let url = Url::from_file_path(&file).unwrap();

        test_pattern_file_finder(url, vec![]).await;
    }

    #[tokio::test]
    async fn test_pattern_file_finder_found() {
        let dir = testdir!();
        let file1 = dir.join("file1.csv");
        let file2 = dir.join("file2.csv");

        create_file(&file2);
        sleep(Duration::from_millis(1200)).await; // Ensure file1 is newer
        create_file(&file1);

        let file = dir.join("file*.csv");
        let url = Url::from_file_path(&file).unwrap();

        let url1 = Url::from_file_path(&file1).unwrap();
        let url2 = Url::from_file_path(&file2).unwrap();
        test_pattern_file_finder(url, vec![url2, url1]).await;
    }

    #[tokio::test]
    async fn test_pattern_file_finder_found_order_by_timestamp() {
        let dir = testdir!();
        let files = (0..5)
            .map(|_| dir.join(format!("{}.csv", Uuid::now_v7())))
            .collect::<Vec<_>>();
        for file in &files {
            create_file(file);
            sleep(Duration::from_millis(1200)).await; // Ensure files are created with a delay
        }

        let file = dir.join("*.csv");
        let url = Url::from_file_path(&file).unwrap();

        let urls = files
            .iter()
            .map(|f| Url::from_file_path(f).unwrap())
            .collect::<Vec<_>>();
        test_pattern_file_finder(url, urls).await;
    }

    async fn test_single_file_last_mod_finder(
        url: Url,
        last_mod: DateTime<Utc>,
        found: Vec<Url>,
    ) -> LastModifiedInfoState {
        let location = Location::LocalFile {
            url: WildcardUrl(url.clone()),
        };
        let lastmod_info = LastModifiedInfoState::new(last_mod);
        let mut finder = SingleFileLastModFinder::new(location, lastmod_info);

        assert_eq!(finder.import_url(), url);

        let files = finder
            .find_files()
            .await
            .unwrap()
            .into_iter()
            .map(|(u, _)| u)
            .collect::<Vec<_>>();
        assert_eq!(files, found);
        finder.lastmod_info().unwrap()
    }

    #[tokio::test]
    async fn test_single_file_last_mod_finder_no_file() {
        let dir = testdir!();
        let file = dir.join("file.csv");
        let url = Url::from_file_path(&file).unwrap();
        let last_mod = UniqueUtc::now_millis();

        let info = test_single_file_last_mod_finder(url.clone(), last_mod, vec![]).await;
        assert!(info.get(url.path()).is_none());
    }

    #[tokio::test]
    async fn test_single_file_last_mod_finder_file_older() {
        let dir = testdir!();
        let file = dir.join("file.csv");
        let url = Url::from_file_path(&file).unwrap();
        create_file(&file);
        sleep(Duration::from_millis(1200)).await;
        let last_mod = UniqueUtc::now_millis();

        let info = test_single_file_last_mod_finder(url.clone(), last_mod, vec![]).await;
        assert!(info.get(url.path()).is_none());
    }

    #[tokio::test]
    async fn test_single_file_last_mod_finder_file_newer() {
        let dir = testdir!();
        let file = dir.join("file.csv");

        let last_mod = UniqueUtc::now_millis();
        sleep(Duration::from_millis(1200)).await;
        create_file(&file);

        let url = Url::from_file_path(&file).unwrap();

        let info = test_single_file_last_mod_finder(url.clone(), last_mod, vec![url.clone()]).await;
        assert_eq!(
            info.get(url.path()).unwrap().1,
            vec!["file.csv".to_string()]
        );
    }

    async fn test_pattern_file_last_mod_finder(
        url: Url,
        last_mod: DateTime<Utc>,
        found: Vec<Url>,
    ) -> LastModifiedInfoState {
        let location = Location::LocalFile {
            url: WildcardUrl(url.clone()),
        };
        let lastmod_info = LastModifiedInfoState::new(last_mod);
        let mut finder = PatternLastModFileFinder::new(location, lastmod_info);

        assert_eq!(finder.import_url(), url);

        let files = finder
            .find_files()
            .await
            .unwrap()
            .into_iter()
            .map(|(u, _)| u)
            .collect::<Vec<_>>();
        assert_eq!(files, found);
        finder.lastmod_info().unwrap()
    }

    #[tokio::test]
    async fn test_pattern_file_last_mod_finder_no_file() {
        let dir = testdir!();
        let file1 = dir.join("file1.csv");
        let file2 = dir.join("file2.csv");

        create_file(&file1);
        create_file(&file2);
        sleep(Duration::from_millis(1200)).await;
        let last_mod = UniqueUtc::now_millis();

        let file = dir.join("file*.csv");
        let url = Url::from_file_path(&file).unwrap();

        let info = test_pattern_file_last_mod_finder(url.clone(), last_mod, vec![]).await;
        assert!(info.get(url.path()).is_none());
    }

    #[tokio::test]
    async fn test_pattern_file_last_mod_finder_file_older() {
        let dir = testdir!();
        let file1 = dir.join("file1.csv");
        let file2 = dir.join("file2.csv");

        create_file(&file1);
        create_file(&file2);
        sleep(Duration::from_millis(1200)).await;
        let last_mod = UniqueUtc::now_millis();

        let file = dir.join("file*.csv");
        let url = Url::from_file_path(&file).unwrap();

        let info = test_pattern_file_last_mod_finder(url.clone(), last_mod, vec![]).await;
        assert!(info.get(url.path()).is_none());
    }

    #[tokio::test]
    async fn test_pattern_file_last_mod_finder_file_newer() {
        let dir = testdir!();
        let file1 = dir.join("file1.csv");
        let file2 = dir.join("file2.csv");

        create_file(&file1);
        sleep(Duration::from_millis(1200)).await;
        let last_mod = UniqueUtc::now_millis();
        sleep(Duration::from_millis(1200)).await;
        create_file(&file2);

        let file = dir.join("file*.csv");
        let url = Url::from_file_path(&file).unwrap();

        let file2 = Url::from_file_path(file2).unwrap();
        let info = test_pattern_file_last_mod_finder(url.clone(), last_mod, vec![file2]).await;
        assert_eq!(
            info.get(url.path()).unwrap().1,
            vec!["file2.csv".to_string()]
        );
    }

    #[tokio::test]
    async fn test_find_and_import() {
        struct TestFilesImporter;

        #[async_trait]
        impl Importer for TestFilesImporter {
            async fn import(
                _import_request: &ImportRequest,
                _files_to_import: Vec<(Url, ObjectMeta)>,
            ) -> Result<Vec<FileImportReport>, TransporterError> {
                Ok(Vec::new())
            }
        }

        let dir = testdir!();
        let file = dir.join("file*.csv");
        let url = Url::from_file_path(&file).unwrap();

        let import_source = ImportSourceBuilder::default()
            .location(Location::LocalFile {
                url: WildcardUrl(url.clone()),
            })
            .initial_lastmod(None)
            .lastmod_info(None)
            .build()
            .unwrap();
        let import_target = ImportTargetBuilder::default()
            .location(Location::LocalFile {
                url: BaseImportUrl(url.clone()),
            })
            .build()
            .unwrap();
        let request = ImportRequestBuilder::default()
            .source(import_source)
            .format(ImportFormat::Json)
            .target(import_target)
            .parallelism(None)
            .build()
            .unwrap();

        let _report = find_and_import::<TestFilesImporter>(request).await.unwrap();
    }
}
