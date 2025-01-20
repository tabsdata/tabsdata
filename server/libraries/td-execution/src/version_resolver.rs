//
// Copyright 2024 Tabs Data Inc.
//

use crate::dataset::{AbsoluteVersion, AbsoluteVersions, TdVersions};
use crate::error::ExecutionPlannerError;
use crate::version_finder::{DsDataVersion, IntoLimitAndOffset, VersionFinder};
use td_common::id::Id;
use td_common::uri::{Version, Versions};

pub struct VersionResolver<'a, VF> {
    version_finder: &'a mut VF,
    td_versions: &'a TdVersions,
}

impl<'a, VF> VersionResolver<'a, VF>
where
    VF: VersionFinder,
{
    pub fn new(version_finder: &'a mut VF, td_versions: &'a TdVersions) -> Self {
        Self {
            version_finder,
            td_versions,
        }
    }

    async fn get_absolute_version(
        &mut self,
        ds_data_version: &Option<DsDataVersion>,
        position_offset: usize,
    ) -> Result<AbsoluteVersion, ExecutionPlannerError> {
        let table_name = self.td_versions.table();
        let table_id = self.version_finder.table_id(table_name).await?;
        let ds_table_id = table_id.map(|t| Id::try_from(t.id()).unwrap());

        let ds_data_version_id = ds_data_version
            .as_ref()
            .map(|v| Id::try_from(v.id()).unwrap());

        let versions = match ds_table_id {
            Some(ds_table_id) => TdVersions::from_table(
                ds_data_version_id,
                ds_table_id,
                self.td_versions.position().unwrap_or(-1), // it should always be Some
            ),
            None => TdVersions::from_dataset(ds_data_version_id),
        };

        let function_id = self.version_finder.function_id().await?;
        let function_id = Id::try_from(function_id.id()).unwrap();

        let version = AbsoluteVersion::new(function_id, versions, position_offset as i64);
        Ok(version)
    }

    /// Returns the head data_version of the dataset, if it exists.
    async fn head_from(
        &mut self,
        offset: isize,
    ) -> Result<Option<DsDataVersion>, ExecutionPlannerError> {
        // Pop because there can only be one version.
        let version = self.version_finder.head_range(1, offset.abs()).await?.pop();
        Ok(version)
    }

    /// Resolves the versions of the dataset and returns the corresponding absolute versions.
    pub async fn resolve(&mut self) -> Result<AbsoluteVersions, ExecutionPlannerError> {
        // We don't need to differ between Plan and Current because the planned datasets are already
        // versioned, at the trigger time.
        let absolute_versions = match self.td_versions.versions() {
            Versions::None => vec![],
            Versions::Single(version) => match version {
                Version::Fixed(id) => {
                    let version = self.version_finder.fixed(id).await?;
                    let version = self.get_absolute_version(&Some(version), 0).await?;
                    vec![version]
                }
                Version::Head(back) => {
                    // We are not failing if a relative version is not found.
                    let version = self.head_from(*back).await?;
                    let version = self.get_absolute_version(&version, 0).await?;
                    vec![version]
                }
            },
            Versions::List(versions) => {
                let mut absolute_versions = vec![];
                for (position, version) in versions.iter().enumerate() {
                    match version {
                        Version::Fixed(id) => {
                            let version = self.version_finder.fixed(id).await?;
                            let version =
                                self.get_absolute_version(&Some(version), position).await?;
                            absolute_versions.push(version)
                        }
                        Version::Head(back) => {
                            // We are not failing if a relative version is not found.
                            let version = self.head_from(*back).await?;
                            let version = self.get_absolute_version(&version, position).await?;
                            absolute_versions.push(version)
                        }
                    }
                }
                absolute_versions
            }
            Versions::Range(from, to) => {
                let (limit, offset) = match (from, to) {
                    (Version::Head(from), Version::Head(to)) => {
                        if from > to {
                            // We do not allow higher to lower ranges in HEAD ranges.
                            Err(ExecutionPlannerError::DecreasingVersionRange(
                                self.td_versions.versions().clone(),
                            ))
                        } else {
                            // We don't need either to exist.
                            (*from, *to).into_limit_and_offset()
                        }
                    }
                    (Version::Fixed(from), Version::Fixed(to)) => {
                        // Both must exist.
                        let from_version = self.version_finder.fixed(from).await?;
                        let from_fixed_offset =
                            self.version_finder.offset_for_fixed(&from_version).await?;

                        let to_version = self.version_finder.fixed(to).await?;
                        let to_fixed_offset =
                            self.version_finder.offset_for_fixed(&to_version).await?;

                        (from_fixed_offset, to_fixed_offset).into_limit_and_offset()
                    }
                    (Version::Fixed(fixed), Version::Head(head)) => {
                        // Fixed must exist, but head doesn't have to.
                        let fixed_version = self.version_finder.fixed(fixed).await?;
                        let fixed_offset =
                            self.version_finder.offset_for_fixed(&fixed_version).await?;

                        (fixed_offset, *head).into_limit_and_offset()
                    }
                    (Version::Head(head), Version::Fixed(fixed)) => {
                        // Fixed must exist, but head doesn't have to.
                        let fixed_version = self.version_finder.fixed(fixed).await?;
                        let fixed_offset =
                            self.version_finder.offset_for_fixed(&fixed_version).await?;

                        (*head, fixed_offset).into_limit_and_offset()
                    }
                }?;

                let mut absolute_versions = vec![];

                // We only return versions that satisfy lower to higher or equal ranges (from <= to).
                // This only can happen if either or both bounds are fixed, because we fail with HEAD ranges.
                // In this scenario, limit is positive, as it marks the direction of the range.
                if limit > 0 {
                    let versions = self
                        .version_finder
                        .head_range(limit.abs(), offset.abs())
                        .await?;

                    // To sort in from -> to order, we reverse the versions.
                    for (position, version) in versions.into_iter().rev().enumerate() {
                        let absolute_version =
                            self.get_absolute_version(&Some(version), position).await?;
                        absolute_versions.push(absolute_version);
                    }
                }

                absolute_versions
            }
        };

        let absolute_versions = AbsoluteVersions::new(absolute_versions);
        Ok(absolute_versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::TdVersions;
    use crate::version_finder::{DsFunction, DsTable, Limit, Offset};
    use async_trait::async_trait;
    use td_common::id;
    use td_common::uri::{Version, Versions};

    // Version finder indexing versions in a vector.
    struct VersionFinderMock {
        function_id: DsFunction,
        table_id: Option<DsTable>,
        versions: Vec<String>,
    }

    impl VersionFinderMock {
        fn new(function_id: &Id, table_id: Option<&Id>) -> Self {
            Self::with_versions(function_id, table_id, Vec::new())
        }

        fn with_versions(function_id: &Id, table_id: Option<&Id>, versions: Vec<String>) -> Self {
            Self {
                function_id: DsFunction::new(&function_id.to_string()),
                table_id: table_id.map(|id| DsTable::new(&id.to_string())),
                versions,
            }
        }
    }

    #[async_trait]
    impl VersionFinder for VersionFinderMock {
        async fn function_id(&mut self) -> Result<&DsFunction, ExecutionPlannerError> {
            Ok(&self.function_id)
        }

        async fn table_id(
            &mut self,
            _table_name: Option<&String>,
        ) -> Result<Option<&DsTable>, ExecutionPlannerError> {
            Ok(self.table_id.as_ref())
        }

        async fn offset_for_fixed(
            &mut self,
            fixed_id: &DsDataVersion,
        ) -> Result<isize, ExecutionPlannerError> {
            self.versions
                .iter()
                .enumerate()
                .find_map(|(offset, id)| {
                    if id == fixed_id.id() {
                        Some(-(offset as isize))
                    } else {
                        None
                    }
                })
                .ok_or(ExecutionPlannerError::CouldNotFetchTable(
                    sqlx::Error::RowNotFound,
                ))
        }

        async fn fixed(&mut self, id: &Id) -> Result<DsDataVersion, ExecutionPlannerError> {
            let fixed = id.to_string();
            self.versions
                .iter()
                .find_map(|id| {
                    if id == &fixed {
                        Some(DsDataVersion::new(&fixed))
                    } else {
                        None
                    }
                })
                .ok_or(ExecutionPlannerError::CouldNotFetchTable(
                    sqlx::Error::RowNotFound,
                ))
        }

        async fn head_range(
            &mut self,
            limit: Limit,
            offset: Offset,
        ) -> Result<Vec<DsDataVersion>, ExecutionPlannerError> {
            let mut versions = vec![];
            for i in offset..(offset + limit) {
                let version = self
                    .versions
                    .get(i as usize)
                    .map(|id| DsDataVersion::new(id));
                if let Some(version) = version {
                    versions.push(version);
                }
            }
            Ok(versions)
        }
    }

    #[tokio::test]
    async fn test_get_absolute_version_with_table_id() {
        let function_id = id::id();
        let table_id = id::id();
        let data_version_id = id::id();

        let versions = TdVersions::from_table(Versions::Single(Version::Head(0)), "t0", -1);

        let mut finder = VersionFinderMock::new(&function_id, Some(&table_id));
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let ds_data_version = DsDataVersion::new(&data_version_id.to_string());
        let absolute_version = resolver
            .get_absolute_version(&Some(ds_data_version), 10)
            .await
            .unwrap();

        assert_eq!(absolute_version.id(), Some(&data_version_id));
        assert_eq!(absolute_version.position(), 10);
        assert_eq!(absolute_version.table_id(), Some(&table_id));
        assert_eq!(absolute_version.function_id(), &function_id);
    }

    #[tokio::test]
    async fn test_get_absolute_version_without_table_id() {
        let function_id = id::id();
        let versions = TdVersions::from_table(Versions::Single(Version::Head(0)), "t0", -1);

        let mut finder = VersionFinderMock::new(&function_id, None);
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let data_version_id = id::id();
        let ds_data_version = DsDataVersion::new(&data_version_id.to_string());
        let absolute_version = resolver
            .get_absolute_version(&Some(ds_data_version), 10)
            .await
            .unwrap();

        assert_eq!(absolute_version.id(), Some(&data_version_id));
        assert_eq!(absolute_version.position(), 10);
        assert!(absolute_version.table_id().is_none());
        assert_eq!(absolute_version.function_id(), &function_id);
    }

    #[tokio::test]
    async fn test_head_from() {
        let function_id = id::id();
        let table_id = id::id();
        let versions = TdVersions::from_table(Versions::Single(Version::Head(0)), "t0", -1);

        let head_0 = id::id();
        let head_1 = id::id();

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let ds_data_version = resolver.head_from(0).await.unwrap();
        assert!(ds_data_version.is_some());
        let ds_data_version = ds_data_version.unwrap();
        assert_eq!(ds_data_version.id(), &head_0.to_string());

        let ds_data_version = resolver.head_from(-1).await.unwrap();
        assert!(ds_data_version.is_some());
        let ds_data_version = ds_data_version.unwrap();
        assert_eq!(ds_data_version.id(), &head_1.to_string());

        // 1 and -1 return the same version. The offset is abs, so it doesn't matter.
        let ds_data_version = resolver.head_from(1).await.unwrap();
        assert!(ds_data_version.is_some());
        let ds_data_version = ds_data_version.unwrap();
        assert_eq!(ds_data_version.id(), &head_1.to_string());

        let ds_data_version = resolver.head_from(-2).await.unwrap();
        assert!(ds_data_version.is_none());

        let ds_data_version = resolver.head_from(-3).await.unwrap();
        assert!(ds_data_version.is_none());
    }

    #[tokio::test]
    async fn test_resolve_single_head() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(Versions::Single(Version::Head(0)), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
        assert_eq!(absolute_versions[0].table_id(), Some(&table_id));
        assert_eq!(absolute_versions[0].function_id(), &function_id);
    }

    #[tokio::test]
    async fn test_resolve_single_head_empty() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(Versions::Single(Version::Head(-1)), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert!(absolute_versions[0].id().is_none());
    }

    #[tokio::test]
    async fn test_resolve_single_fixed() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(Versions::Single(Version::Fixed(head_0)), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_list_single_head() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(Versions::List(vec![Version::Head(0)]), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_list_single_head_empty() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(Versions::List(vec![Version::Head(-1)]), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert!(absolute_versions[0].id().is_none());
    }

    #[tokio::test]
    async fn test_resolve_list_single_fixed() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions =
            TdVersions::from_table(Versions::List(vec![Version::Fixed(head_0)]), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_list_single_fixed_empty() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();
        let head_1 = id::id();

        let versions =
            TdVersions::from_table(Versions::List(vec![Version::Fixed(head_1)]), "t0", 0);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await;
        assert!(matches!(
            absolute_versions,
            Err(ExecutionPlannerError::CouldNotFetchTable(_))
        ));
    }

    #[tokio::test]
    async fn test_resolve_list_full() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();
        let head_1 = id::id();

        let versions = TdVersions::from_table(
            Versions::List(vec![Version::Head(0), Version::Head(-1)]),
            "t0",
            0,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
        assert_eq!(absolute_versions[1].id(), Some(&head_1));
    }

    #[tokio::test]
    async fn test_resolve_list_full_fixed() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();
        let head_1 = id::id();

        let versions = TdVersions::from_table(
            Versions::List(vec![Version::Fixed(head_0), Version::Fixed(head_1)]),
            "t0",
            0,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
        assert_eq!(absolute_versions[1].id(), Some(&head_1));
    }

    #[tokio::test]
    async fn test_resolve_list_incomplete_head() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(
            Versions::List(vec![Version::Head(0), Version::Head(-1)]),
            "t0",
            0,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
        assert!(absolute_versions[1].id().is_none());
    }

    #[tokio::test]
    async fn test_resolve_list_incomplete_fixed() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions =
            TdVersions::from_table(Versions::List(vec![Version::Fixed(head_0)]), "t0", 0);

        let mut finder = VersionFinderMock::new(&function_id, Some(&table_id));
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await;
        assert!(matches!(
            absolute_versions,
            Err(ExecutionPlannerError::CouldNotFetchTable(_))
        ));
    }

    #[tokio::test]
    async fn test_resolve_list_incomplete_head_and_fixed() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(
            Versions::List(vec![Version::Head(-2), Version::Fixed(head_0)]),
            "t0",
            0,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), None);
        assert_eq!(absolute_versions[1].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_single() {
        let function_id = id::id();
        let table_id = id::id();
        let versions = TdVersions::from_table(
            Versions::Range(Version::Head(0), Version::Head(0)),
            "t0",
            -1,
        );

        let head_0 = id::id();
        let head_1 = id::id();

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_full() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();
        let head_1 = id::id();

        let versions = TdVersions::from_table(
            Versions::Range(Version::Head(-1), Version::Head(0)),
            "t0",
            -1,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_1));
        assert_eq!(absolute_versions[1].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_incomplete() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();
        let head_1 = id::id();

        let versions = TdVersions::from_table(
            Versions::Range(Version::Head(-2), Version::Head(0)),
            "t0",
            -1,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_1));
        assert_eq!(absolute_versions[1].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_head_empty() {
        let function_id = id::id();
        let table_id = id::id();
        let head_0 = id::id();

        let versions = TdVersions::from_table(
            Versions::Range(Version::Head(-2), Version::Head(-1)),
            "t0",
            0,
        );

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);

        let absolute_versions = resolver.resolve().await.unwrap();
        assert_eq!(absolute_versions.len(), 0);
    }

    async fn test_resolve_range_between(
        versions: Versions,
        head_0: &Id,
        head_1: &Id,
    ) -> Result<AbsoluteVersions, ExecutionPlannerError> {
        let function_id = id::id();
        let table_id = id::id();
        let versions = TdVersions::from_table(versions, "t0", -1);

        let mut finder = VersionFinderMock::with_versions(
            &function_id,
            Some(&table_id),
            vec![head_0.to_string(), head_1.to_string()],
        );
        let mut resolver = VersionResolver::new(&mut finder, &versions);
        resolver.resolve().await
    }

    #[tokio::test]
    async fn test_resolve_range_equal() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(0), Version::Head(0));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_equal_fixed() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Fixed(head_0), Version::Fixed(head_0));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_equal_fixed_relative() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(0), Version::Fixed(head_0));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_equal_relative() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(-1), Version::Head(-1));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0].id(), Some(&head_1));
    }

    #[tokio::test]
    async fn test_resolve_range_greater_to_lower() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(0), Version::Head(-1));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1).await;

        match absolute_versions.unwrap_err() {
            ExecutionPlannerError::DecreasingVersionRange(versions) => {
                assert_eq!(
                    versions,
                    Versions::Range(Version::Head(0), Version::Head(-1))
                );
            }
            _ => panic!("Expected ExecutionPlannerError::DecreasingVersionRange"),
        }
    }

    #[tokio::test]
    async fn test_resolve_range_lower_to_greater() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(-1), Version::Head(0));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_1));
        assert_eq!(absolute_versions[1].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_greater_to_fixed() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(0), Version::Fixed(head_1));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        // We only get versions that satisfy lower to higher ranges.
        assert_eq!(absolute_versions.len(), 0);
    }

    #[tokio::test]
    async fn test_resolve_range_fixed_to_greater() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Fixed(head_1), Version::Head(0));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_1));
        assert_eq!(absolute_versions[1].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_lower_to_fixed() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Head(-1), Version::Fixed(head_0));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        assert_eq!(absolute_versions.len(), 2);
        assert_eq!(absolute_versions[0].id(), Some(&head_1));
        assert_eq!(absolute_versions[1].id(), Some(&head_0));
    }

    #[tokio::test]
    async fn test_resolve_range_fixed_to_lower() {
        let head_0 = id::id();
        let head_1 = id::id();
        let versions = Versions::Range(Version::Fixed(head_0), Version::Head(-1));

        let absolute_versions = test_resolve_range_between(versions, &head_0, &head_1)
            .await
            .unwrap();

        // We only get versions that satisfy lower to higher ranges.
        assert_eq!(absolute_versions.len(), 0);
    }
}
