//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(test)]
mod tests {
    use crate as ta_objects;

    use crate::dxo::{Buildable, Extractor, TryUpdater};
    use crate::{Dao, Dlo, DloType, Dto};

    #[tm_objects::typed(string(default = "default"))]
    #[derive(Eq, PartialEq)]
    struct UserId;

    #[Dao]
    #[dao(sql_table = "foo_dao")]
    struct FooDao {
        id: UserId,
        name: UserId,
        description: Option<UserId>,
    }

    #[Dlo]
    struct FooDlo {
        id: UserId,
    }

    #[Dto]
    struct FooDto {
        name: UserId,
        description: Option<UserId>,
    }

    #[test]
    fn test_dao() -> Result<(), td_error::TdError> {
        #[Dao]
        #[dao(sql_table = "test_dao")]
        #[dxo(builder(try_from = FooDao))]
        struct TestDao {
            name: UserId,
        }

        let id = UserId::default();
        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(id)
            .name(name.clone())
            .description(desc)
            .build()?;

        let dao = TestDaoBuilder::try_from(&dao)?.build()?;
        assert_eq!(dao.name, name);
        Ok(())
    }

    #[test]
    fn test_dto() -> Result<(), td_error::TdError> {
        #[Dto]
        #[dxo(builder(try_from = FooDto))]
        struct TestDto {
            name: UserId,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dto = FooDto::builder()
            .name(name.clone())
            .description(desc)
            .build()?;

        let dto = TestDtoBuilder::try_from(&dto)?.build()?;
        assert_eq!(dto.name, name);
        Ok(())
    }

    #[test]
    fn test_dlo() -> Result<(), td_error::TdError> {
        #[Dlo]
        #[dxo(builder(try_from = FooDlo))]
        struct TestDlo {
            id: UserId,
        }

        let id = UserId::default();
        let dlo = FooDlo::builder().id(id.clone()).build()?;

        let dlo = TestDloBuilder::try_from(&dlo)?.build()?;
        assert_eq!(dlo.id, id);
        Ok(())
    }

    #[test]
    fn test_into_builder() {
        let id = UserId::default();
        let name = UserId::default();
        let desc = UserId::default();
        let new_name = UserId::default();

        let dao = FooDao::builder()
            .id(id.clone())
            .name(name)
            .description(desc.clone())
            .build()
            .unwrap();

        assert_eq!(dao.description, Some(desc.clone()));

        let new_dao = dao.into_builder().name(new_name.clone()).build().unwrap();

        assert_eq!(new_dao.id, id);
        assert_eq!(new_dao.name, new_name);
        assert_eq!(new_dao.description, Some(desc));
    }

    #[test]
    fn test_builder_try_from() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao))]
        struct TestDto {
            name: UserId,
            description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(UserId::default())
            .name(name.clone())
            .description(desc.clone())
            .build()
            .unwrap();

        let dto = TestDtoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(*dto.name(), name);
        assert_eq!(*dto.description(), Some(desc));
    }

    #[test]
    fn test_builder_from_skip() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao))]
        struct TestDto {
            name: UserId,
            #[dxo(builder(skip))]
            description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(UserId::default())
            .name(name.clone())
            .description(desc)
            .build()
            .unwrap();

        let dto_builder = TestDtoBuilder::try_from(&dao).unwrap();
        assert_eq!(dto_builder.name, Some(name.clone()));
        assert_eq!(dto_builder.description, None);

        let new_desc = UserId::default();
        let dto = TestDtoBuilder::try_from(&dao)
            .unwrap()
            .description(Some(new_desc.clone()))
            .build()
            .unwrap();
        assert_eq!(*dto.name(), name);
        assert_eq!(*dto.description(), Some(new_desc));
    }

    #[test]
    fn test_builder_from_skip_all_and_include() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao, skip_all))]
        struct TestDto {
            #[dxo(builder(include))]
            name: UserId,
            description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(UserId::default())
            .name(name.clone())
            .description(desc)
            .build()
            .unwrap();

        let dto_builder = TestDtoBuilder::try_from(&dao).unwrap();
        assert_eq!(dto_builder.name, Some(name.clone()));
        assert_eq!(dto_builder.description, None);

        let new_desc = UserId::default();
        let dto = TestDtoBuilder::try_from(&dao)
            .unwrap()
            .description(Some(new_desc.clone()))
            .build()
            .unwrap();
        assert_eq!(*dto.name(), name);
        assert_eq!(*dto.description(), Some(new_desc));
    }

    #[test]
    fn test_builder_from_combined() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao, skip_all))]
        #[dxo(builder(try_from = FooDto))]
        struct TestDxo {
            #[dxo(builder(try_from = FooDao, include))]
            name: UserId,
            #[dxo(builder(try_from = FooDao, include))]
            #[dxo(builder(try_from = FooDto, skip))]
            description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(UserId::default())
            .name(name.clone())
            .description(desc.clone())
            .build()
            .unwrap();

        let dxo = TestDxoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(*dxo.name(), name);
        assert_eq!(*dxo.description(), Some(desc));

        let dto_name = UserId::default();
        let dto_desc = UserId::default();
        let dto = FooDto::builder()
            .name(dto_name.clone())
            .description(dto_desc)
            .build()
            .unwrap();

        let dxo_builder = TestDxoBuilder::try_from(&dto).unwrap();
        assert_eq!(dxo_builder.name, Some(dto_name));
        assert_eq!(dxo_builder.description, None);
    }

    #[test]
    fn test_builder_from_combined_all() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao, skip_all))]
        #[dxo(builder(try_from = FooDto))]
        #[dxo(builder(try_from = FooDlo))]
        struct TestDxo {
            #[dxo(builder(try_from = FooDao, include))]
            #[dxo(builder(try_from = FooDto, skip))]
            id: UserId,
            #[dxo(builder(try_from = FooDao, include))]
            #[dxo(builder(try_from = FooDlo, skip))]
            #[builder(default)]
            name: UserId,
            #[dxo(builder(try_from = FooDao, include))]
            #[dxo(builder(try_from = FooDto, skip))]
            #[dxo(builder(try_from = FooDlo, skip))]
            #[builder(default)]
            description: Option<UserId>,
            #[dxo(builder(try_from = FooDao, include, field = "name"))]
            #[dxo(builder(try_from = FooDto, field = "name"))]
            #[dxo(builder(try_from = FooDlo, skip))]
            new_name: UserId,
        }

        let id = UserId::default();
        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(id.clone())
            .name(name.clone())
            .description(desc.clone())
            .build()
            .unwrap();

        let dxo = TestDxoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(dxo.id, id);
        assert_eq!(*dxo.name(), name);
        assert_eq!(*dxo.description(), Some(desc));
        assert_eq!(*dxo.new_name(), name);

        let dto_name = UserId::default();
        let dto = FooDto::builder()
            .name(dto_name.clone())
            .description(UserId::default())
            .build()
            .unwrap();

        let dxo_builder = TestDxoBuilder::try_from(&dto).unwrap();
        assert_eq!(dxo_builder.name, Some(dto_name.clone()));
        assert_eq!(dxo_builder.description, None);
        assert_eq!(dxo_builder.new_name, Some(dto_name));

        let dlo_id = UserId::default();
        let dlo = FooDlo::builder().id(dlo_id.clone()).build().unwrap();

        let dxo_builder = TestDxoBuilder::try_from(&dlo).unwrap();
        assert_eq!(dxo_builder.id, Some(dlo_id));
        assert_eq!(dxo_builder.name, None);
        assert_eq!(dxo_builder.description, None);
        assert_eq!(dxo_builder.new_name, None);
    }

    #[test]
    fn test_from_error() -> Result<(), td_error::TdError> {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao))]
        struct TestDto {
            name: UserId,
            #[dxo(builder(field = "description"))]
            renamed_description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(UserId::default())
            .name(name.clone())
            .description(desc.clone())
            .build()?;

        let dto = TestDtoBuilder::try_from(&dao)?.build()?;
        assert_eq!(*dto.name(), name);
        assert_eq!(*dto.renamed_description(), Some(desc));
        Ok(())
    }

    #[test]
    fn test_updated_from() -> Result<(), td_error::TdError> {
        #[Dto]
        struct TestDto {
            id: UserId,
        }

        #[Dao]
        #[dao(sql_table = "test_updated_from")]
        #[dxo(updater(try_from = TestDto, skip_all))]
        struct TestDao {
            #[dxo(updater(include))]
            id: UserId,
            name: UserId,
        }

        let id = UserId::default();
        let name = UserId::default();
        let dto = TestDto::builder().id(id.clone()).build()?;

        let dao = TestDao::builder()
            .try_update_from(&dto)?
            .name(name.clone())
            .build()?;

        assert_eq!(dao.id, id);
        assert_eq!(dao.name, name);
        Ok(())
    }

    #[test]
    fn test_try_from_updated_from() -> Result<(), td_error::TdError> {
        #[Dlo]
        struct TestDlo {
            name: UserId,
            description: Option<UserId>,
        }

        #[Dto]
        struct TestDto {
            id: UserId,
        }

        #[Dao]
        #[dao(sql_table = "test_try_from_updated_from")]
        #[dxo(builder(try_from = TestDlo))]
        #[dxo(updater(try_from = TestDto, skip_all))]
        struct TestDao {
            #[dxo(builder(skip))]
            #[dxo(updater(include))]
            id: UserId,
            name: UserId,
            description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();
        let id = UserId::default();

        let dlo = TestDlo::builder()
            .name(name.clone())
            .description(desc.clone())
            .build()?;

        let dto = TestDto::builder().id(id.clone()).build()?;

        let dao = TestDaoBuilder::try_from(&dlo)?
            .try_update_from(&dto)?
            .build()?;

        assert_eq!(dao.id, id);
        assert_eq!(dao.name, name);
        assert_eq!(dao.description, Some(desc));
        Ok(())
    }

    #[test]
    fn test_try_from_updated_from_default() -> Result<(), td_error::TdError> {
        #[Dlo]
        struct TestDlo {
            name: UserId,
            description: Option<UserId>,
        }

        #[Dto]
        struct TestDto {
            id: UserId,
        }

        #[Dao]
        #[dao(sql_table = "test_try_from_updated_from_default")]
        #[dxo(builder(try_from = TestDlo))]
        #[dxo(updater(try_from = TestDto, skip_all))]
        struct TestDao {
            #[dxo(builder(skip))]
            #[dxo(updater(include))]
            id: UserId,
            name: UserId,
            description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();
        let id = UserId::default();

        let dlo = TestDlo::builder()
            .name(name.clone())
            .description(desc.clone())
            .build()?;

        let dto = TestDto::builder().id(id.clone()).build()?;
        let dao = TestDaoBuilder::try_from(&dlo)?
            .try_update_from(&dto)?
            .build()?;

        assert_eq!(dao.id, id);
        assert_eq!(dao.name, name);
        assert_eq!(dao.description, Some(desc));
        Ok(())
    }

    #[test]
    fn test_ta_objects_extractor() -> Result<(), td_error::TdError> {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        struct TestDxo {
            #[dxo(extractor)]
            name: String,
            _size: i64,
        }

        let dxo = TestDxo::builder().name("name")._size(123).build()?;

        let name = dxo.extract();
        assert_eq!(name, "name");
        Ok(())
    }

    #[test]
    fn test_builder_from_default() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao))]
        struct TestDto {
            #[dxo(builder(skip))]
            #[builder(default)]
            name: Option<UserId>,
            description: Option<UserId>,
        }

        let desc = UserId::default();
        let dao = FooDao::builder()
            .id(UserId::default())
            .name(UserId::default())
            .description(desc.clone())
            .build()
            .unwrap();

        let dto = TestDtoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(*dto.name(), None);
        assert_eq!(*dto.description(), Some(desc));
    }

    #[test]
    fn test_builder_from_rename() {
        #[derive(Debug, Default, DloType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[dxo(builder(try_from = FooDao))]
        struct TestDto {
            name: UserId,
            #[dxo(builder(field = "description"))]
            renamed_description: Option<UserId>,
        }

        let name = UserId::default();
        let desc = UserId::default();

        let dao = FooDao::builder()
            .id(UserId::default())
            .name(name.clone())
            .description(desc.clone())
            .build()
            .unwrap();

        let dto = TestDtoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(*dto.name(), name);
        assert_eq!(*dto.renamed_description(), Some(desc));
    }
}
