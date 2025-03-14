//
// Copyright 2025 Tabs Data Inc.
//

#[cfg(test)]
mod tests {
    use crate::types::DataAccessObject;
    use td_type::{Dao, Dlo, Dto, TdType};

    #[Dao]
    struct FooDao {
        id: i64,
        name: String,
        description: Option<String>,
        modified: chrono::DateTime<chrono::Utc>,
        active: bool,
    }

    #[Dlo]
    struct FooDlo {
        id: i64,
    }

    #[Dto]
    struct FooDto {
        name: String,
        description: Option<String>,
    }

    #[test]
    fn test_dao() -> Result<(), td_error::TdError> {
        #[Dao]
        #[td_type(builder(try_from = FooDao))]
        struct TestDao {
            name: String,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()?;

        let dao = TestDaoBuilder::try_from(&dao)?.build()?;
        assert_eq!(dao.name, "dao");
        Ok(())
    }

    #[test]
    fn test_dto() -> Result<(), td_error::TdError> {
        #[Dto]
        #[td_type(builder(try_from = FooDto))]
        struct TestDto {
            name: String,
        }

        let dto = FooDto::builder()
            .name("dao")
            .description("dao desc".to_string())
            .build()?;

        let dto = TestDtoBuilder::try_from(&dto)?.build()?;
        assert_eq!(dto.name, "dao");
        Ok(())
    }

    #[test]
    fn test_dlo() -> Result<(), td_error::TdError> {
        #[Dlo]
        #[td_type(builder(try_from = FooDlo))]
        struct TestDlo {
            id: i128,
        }

        let dlo = FooDlo::builder().id(1234).build()?;

        let dlo = TestDloBuilder::try_from(&dlo)?.build()?;
        assert_eq!(dlo.id, 1234);
        Ok(())
    }

    #[test]
    fn test_to_builder() {
        let modified = chrono::DateTime::<chrono::Utc>::default();
        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(modified)
            .active(true)
            .build()
            .unwrap();

        assert_eq!(dao.id, 123);
        assert_eq!(dao.name, "dao");
        assert_eq!(dao.description, Some("dao desc".to_string()));
        assert_eq!(dao.modified, modified);
        assert!(dao.active);

        let new_dao = dao.to_builder().name("renamed dao").build().unwrap();

        assert_eq!(new_dao.id, 123);
        assert_eq!(new_dao.name, "renamed dao");
        assert_eq!(new_dao.description, Some("dao desc".to_string()));
        assert_eq!(new_dao.modified, modified);
        assert!(new_dao.active);
    }

    #[test]
    fn test_fields() {
        #[Dao]
        struct Dao {
            id: i64,
            name: String,
            description: Option<String>,
            modified: chrono::DateTime<chrono::Utc>,
            active: bool,
        }

        let fields = Dao::fields();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0], "id");
        assert_eq!(fields[1], "name");
        assert_eq!(fields[2], "description");
        assert_eq!(fields[3], "modified");
        assert_eq!(fields[4], "active");
    }

    // #[test]
    // fn test_fields_skip() {
    //     #[Dao]
    //     struct Dao {
    //         id: i64,
    //         #[sqlx(skip)]
    //         name: String,
    //         #[sqlx(flatten)]
    //         description: Option<String>,
    //         #[sqlx(skip)]
    //         modified: chrono::DateTime<chrono::Utc>,
    //         active: bool,
    //     }
    //
    //     let fields = Dao::fields();
    //     assert_eq!(fields.len(), 3);
    //     assert_eq!(fields[0], "id");
    //     assert_eq!(fields[1], "description");
    //     assert_eq!(fields[2], "active");
    // }

    #[test]
    fn test_builder_try_from() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao))]
        struct TestDto {
            name: String,
            description: Option<String>,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let dto = TestDtoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(dto.name, "dao");
        assert_eq!(dto.description, Some("dao desc".to_string()));
    }

    #[test]
    fn test_builder_from_skip() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao))]
        struct TestDto {
            name: String,
            #[td_type(builder(skip))]
            description: Option<String>,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let mut dto_builder = TestDtoBuilder::try_from(&dao).unwrap();
        assert_eq!(dto_builder.name, Some("dao".to_string()));
        assert_eq!(dto_builder.description, None);

        let dto = dto_builder
            .description(Some("my new dao desc".to_string()))
            .build()
            .unwrap();
        assert_eq!(dto.name, "dao");
        assert_eq!(dto.description, Some("my new dao desc".to_string()));
    }

    #[test]
    fn test_builder_from_skip_all_and_include() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao, skip_all))]
        struct TestDto {
            #[td_type(builder(include))]
            name: String,
            description: Option<String>,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let mut dto_builder = TestDtoBuilder::try_from(&dao).unwrap();
        assert_eq!(dto_builder.name, Some("dao".to_string()));
        assert_eq!(dto_builder.description, None);

        let dto = dto_builder
            .description(Some("my new dao desc".to_string()))
            .build()
            .unwrap();
        assert_eq!(dto.name, "dao");
        assert_eq!(dto.description, Some("my new dao desc".to_string()));
    }

    #[test]
    fn test_builder_from_combined() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao, skip_all))]
        #[td_type(builder(try_from = FooDto))]
        struct TestDxo {
            #[td_type(builder(try_from = FooDao, include))]
            name: String,
            #[td_type(builder(try_from = FooDao, include))]
            #[td_type(builder(try_from = FooDto, skip))]
            description: Option<String>,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let dxo = TestDxoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(dxo.name, "dao".to_string());
        assert_eq!(dxo.description, Some("dao desc".to_string()));

        let dto = FooDto::builder()
            .name("dao")
            .description("dao desc".to_string())
            .build()
            .unwrap();

        let mut dxo_builder = TestDxoBuilder::try_from(&dto).unwrap();
        assert_eq!(dxo_builder.name, Some("dao".to_string()));
        assert_eq!(dxo_builder.description, None);

        let dxo = dxo_builder
            .description(Some("my new dxo desc".to_string()))
            .build()
            .unwrap();
        assert_eq!(dxo.name, "dao");
        assert_eq!(dxo.description, Some("my new dxo desc".to_string()));
    }

    #[test]
    fn test_builder_from_default() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao))]
        struct TestDto {
            #[td_type(builder(default))]
            name: String,
            description: Option<String>,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let dto = TestDtoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(dto.name, String::default());
        assert_eq!(dto.description, Some("dao desc".to_string()));
    }

    #[test]
    fn test_builder_from_rename() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao))]
        struct TestDto {
            name: String,
            #[td_type(builder(field = "description"))]
            renamed_description: Option<String>,
            #[td_type(builder(field = "active"))]
            actually_active: bool,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let dto = TestDtoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(dto.name, "dao");
        assert_eq!(dto.renamed_description, Some("dao desc".to_string()));
        assert!(dto.actually_active);
    }

    #[test]
    fn test_builder_from_combined_all() {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao, skip_all))]
        #[td_type(builder(try_from = FooDto))]
        #[td_type(builder(try_from = FooDlo))]
        struct TestDxo {
            #[td_type(builder(try_from = FooDao, include))]
            #[td_type(builder(try_from = FooDto, skip))]
            id: i64,
            #[td_type(builder(try_from = FooDao, include))]
            #[td_type(builder(try_from = FooDlo, default))]
            name: String,
            #[td_type(builder(try_from = FooDao, include))]
            #[td_type(builder(try_from = FooDto, skip))]
            #[td_type(builder(try_from = FooDlo, default))]
            description: Option<String>,
            #[td_type(builder(try_from = FooDao, include, field = "name"))]
            #[td_type(builder(try_from = FooDto, field = "name"))]
            #[td_type(builder(try_from = FooDlo, skip))]
            new_name: String,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()
            .unwrap();

        let dxo = TestDxoBuilder::try_from(&dao).unwrap().build().unwrap();
        assert_eq!(dxo.id, 123);
        assert_eq!(dxo.name, "dao".to_string());
        assert_eq!(dxo.description, Some("dao desc".to_string()));
        assert_eq!(dxo.new_name, "dao".to_string());

        let dto = FooDto::builder()
            .name("dto")
            .description("dto desc".to_string())
            .build()
            .unwrap();

        let mut dxo_builder = TestDxoBuilder::try_from(&dto).unwrap();
        assert_eq!(dxo_builder.id, None);
        assert_eq!(dxo_builder.name, Some("dto".to_string()));
        assert_eq!(dxo_builder.description, None);
        assert_eq!(dxo_builder.new_name, Some("dto".to_string()));

        let dxo = dxo_builder
            .id(456)
            .name("new name".to_string())
            .description(Some("new description".to_string()))
            .build()
            .unwrap();
        assert_eq!(dxo.id, 456);
        assert_eq!(dxo.name, "new name".to_string());
        assert_eq!(dxo.description, Some("new description".to_string()));
        assert_eq!(dxo.new_name, "dto".to_string());

        let dlo = FooDlo::builder().id(789).build().unwrap();

        let mut dxo_builder = TestDxoBuilder::try_from(&dlo).unwrap();
        assert_eq!(dxo_builder.id, Some(789));
        assert_eq!(dxo_builder.name, Some("".to_string()));
        assert_eq!(dxo_builder.description, Some(None));
        assert_eq!(dxo_builder.new_name, None);

        let dxo = dxo_builder
            .description(Some("new description".to_string()))
            .new_name("new name".to_string())
            .build()
            .unwrap();
        assert_eq!(dxo.id, 789);
        assert_eq!(dxo.name, "".to_string());
        assert_eq!(dxo.description, Some("new description".to_string()));
        assert_eq!(dxo.new_name, "new name".to_string());
    }

    #[test]
    fn test_from_error() -> Result<(), td_error::TdError> {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #[td_type(builder(try_from = FooDao))]
        struct TestDto {
            name: String,
            #[td_type(builder(field = "description"))]
            renamed_description: Option<String>,
            #[td_type(builder(field = "active"))]
            actually_active: bool,
        }

        let dao = FooDao::builder()
            .id(123)
            .name("dao")
            .description("dao desc".to_string())
            .modified(chrono::DateTime::<chrono::Utc>::default())
            .active(true)
            .build()?;

        let dto = TestDtoBuilder::try_from(&dao)?.build()?;
        assert_eq!(dto.name, "dao");
        assert_eq!(dto.renamed_description, Some("dao desc".to_string()));
        assert!(dto.actually_active);
        Ok(())
    }

    #[test]
    fn test_updated_from() -> Result<(), td_error::TdError> {
        #[Dto]
        struct TestDto {
            id: i64,
        }

        #[Dao]
        #[td_type(updater(try_from = TestDto, skip_all))]
        struct TestDao {
            #[td_type(updater(include))]
            id: i64,
            name: String,
            description: Option<String>,
            modified: chrono::DateTime<chrono::Utc>,
            active: bool,
        }

        let dto = TestDto::builder().id(123).build()?;

        let now = chrono::Utc::now();
        let mut dao_builder = TestDao::builder();
        dao_builder
            .name("dlo")
            .description(Some("desc".to_string()))
            .modified(now)
            .active(false);
        let dao = TestDaoBuilder::try_from((&dto, dao_builder))?.build()?;

        assert_eq!(dao.id, 123);
        assert_eq!(dao.name, "dlo");
        assert_eq!(dao.description, Some("desc".to_string()));
        assert_eq!(dao.modified, now);
        assert!(!dao.active);
        Ok(())
    }

    #[test]
    fn test_try_from_updated_from() -> Result<(), td_error::TdError> {
        #[Dlo]
        struct TestDlo {
            name: String,
            description: Option<String>,
            modified: chrono::DateTime<chrono::Utc>,
        }

        #[Dto]
        struct TestDto {
            id: i64,
        }

        #[Dao]
        #[td_type(builder(try_from = TestDlo))]
        #[td_type(updater(try_from = TestDto, skip_all))]
        struct TestDao {
            #[td_type(builder(skip))]
            #[td_type(updater(include))]
            id: i64,
            name: String,
            description: Option<String>,
            modified: chrono::DateTime<chrono::Utc>,
            #[td_type(builder(skip))]
            active: bool,
        }

        let now = chrono::Utc::now();
        let dlo = TestDlo::builder()
            .name("dlo")
            .description(Some("desc".to_string()))
            .modified(now)
            .build()?;

        let dto = TestDto::builder().id(123).build()?;

        let builder = TestDaoBuilder::try_from(&dlo)?;
        let dao = TestDaoBuilder::try_from((&dto, builder))?
            .active(true)
            .build()?;

        assert_eq!(dao.id, 123);
        assert_eq!(dao.name, "dlo");
        assert_eq!(dao.description, Some("desc".to_string()));
        assert_eq!(dao.modified, now);
        assert!(dao.active);
        Ok(())
    }

    #[test]
    fn test_try_from_updated_from_default() -> Result<(), td_error::TdError> {
        #[Dlo]
        struct TestDlo {
            name: String,
            description: Option<String>,
            modified: chrono::DateTime<chrono::Utc>,
        }

        #[Dto]
        struct TestDto {
            id: i64,
        }

        #[Dao]
        #[td_type(builder(try_from = TestDlo))]
        #[td_type(updater(try_from = TestDto, skip_all))]
        struct TestDao {
            #[td_type(builder(skip))]
            #[td_type(updater(include))]
            id: i64,
            name: String,
            description: Option<String>,
            modified: chrono::DateTime<chrono::Utc>,
            #[td_type(builder(default))]
            active: bool,
        }

        let now = chrono::Utc::now();
        let dlo = TestDlo::builder()
            .name("dlo")
            .description(Some("desc".to_string()))
            .modified(now)
            .build()?;

        let dto = TestDto::builder().id(123).build()?;
        let builder = TestDaoBuilder::try_from(&dlo)?;
        let dao = TestDaoBuilder::try_from((&dto, builder))?.build()?;

        assert_eq!(dao.id, 123);
        assert_eq!(dao.name, "dlo");
        assert_eq!(dao.description, Some("desc".to_string()));
        assert_eq!(dao.modified, now);
        assert!(!dao.active);
        Ok(())
    }

    #[test]
    fn test_td_type_extractor() -> Result<(), td_error::TdError> {
        #[derive(Debug, Default, TdType, derive_builder::Builder, getset::Getters)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        struct TestDxo {
            #[td_type(extractor)]
            name: String,
            _size: i64,
        }

        let dxo = TestDxo::builder().name("name")._size(123).build()?;

        let name = String::from(&dxo);
        assert_eq!(name, "name");
        Ok(())
    }
}
