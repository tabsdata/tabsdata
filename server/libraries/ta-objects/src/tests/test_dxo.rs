//
// Copyright 2026 Tabs Data Inc.
//

/// Tests for the `#[dxo]` module-level macro: verifies field inheritance via
/// `#[inherits]`, field exclusion via `#[exclude]`, and attribute merging.
#[cfg(test)]
mod tests {
    use crate as ta_objects;

    use crate::dxo::{Buildable, Dao, DaoCore, VersionedDao};
    use sea_query::Iden;

    // ---- Typed helpers -------------------------------------------------------

    #[tm_objects::typed(string(default = "default"))]
    #[derive(Eq, PartialEq)]
    struct TestId;

    #[tm_objects::typed(string(default = "name"))]
    #[derive(Eq, PartialEq)]
    struct TestName;

    #[tm_objects::typed(string(default = "desc"))]
    #[derive(Eq, PartialEq)]
    struct TestDescription;

    #[tm_objects::typed(i64(default = 0))]
    #[derive(Eq, PartialEq)]
    struct TestVersion;

    // ---- Test: basic field inheritance (Dao → Dao) --------------------------

    #[ta_objects::dxo]
    mod dxo_basic_inherit {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "base_table")]
        pub struct BaseDao {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "derived_table")]
        #[inherits(BaseDao)]
        pub struct DerivedDao {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dao_inherits_fields_from_base() -> Result<(), td_error::TdError> {
        // DerivedDao should have: id, name (from BaseDao), description (own)
        let derived = DerivedDao::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(derived.id, TestId::default());
        assert_eq!(derived.name, TestName::default());
        assert_eq!(derived.description, TestDescription::default());
        Ok(())
    }

    #[test]
    fn test_inherited_dao_has_correct_columns() {
        // DerivedDao should have columns from base + own
        let cols: Vec<_> = DerivedDao::columns().into_iter().collect();
        let col_names: Vec<_> = cols.iter().map(|c| c.quoted()).collect();

        assert!(col_names.contains(&"id".into()));
        assert!(col_names.contains(&"name".into()));
        assert!(col_names.contains(&"description".into()));
        assert_eq!(col_names.len(), 3);
    }

    #[test]
    fn test_inherited_dao_uses_own_table() {
        // DerivedDao should use its own sql_table, not the base's
        let table = DerivedDao::table();
        assert_eq!(table.quoted(), "derived_table");
    }

    // ---- Test: #[exclude] skips fields during inheritance -------------------

    #[ta_objects::dxo]
    mod dxo_exclude {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "base_with_extra")]
        pub struct BaseWithExtra {
            pub id: TestId,
            pub name: TestName,
            pub version: TestVersion,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "derived_excluding")]
        #[inherits(BaseWithExtra)]
        pub struct DerivedExcluding {
            #[exclude]
            pub version: TestVersion,

            pub description: TestDescription,
        }
    }

    #[test]
    fn test_exclude_removes_field_from_inheritance() {
        // DerivedExcluding should have: id, name (inherited), description (own)
        // but NOT version (excluded)
        let cols: Vec<_> = DerivedExcluding::columns().into_iter().collect();
        let col_names: Vec<_> = cols.iter().map(|c| c.quoted()).collect();

        assert!(col_names.contains(&"id".into()));
        assert!(col_names.contains(&"name".into()));
        assert!(col_names.contains(&"description".into()));
        assert!(!col_names.contains(&"version".into()));
        assert_eq!(col_names.len(), 3);
    }

    #[test]
    fn test_exclude_struct_is_buildable() -> Result<(), td_error::TdError> {
        // Verify the builder doesn't require the excluded field
        let derived = DerivedExcluding::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(derived.id, TestId::default());
        assert_eq!(derived.name, TestName::default());
        assert_eq!(derived.description, TestDescription::default());
        Ok(())
    }

    // ---- Test: Dao → Dto cross-type inheritance -----------------------------

    #[ta_objects::dxo]
    mod dxo_cross_type {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "cross_base")]
        pub struct CrossBaseDao {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dto]
        #[inherits(CrossBaseDao)]
        pub struct CrossDerivedDto {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dao_to_dto_inherits_fields() -> Result<(), td_error::TdError> {
        // DTO inherits fields from DAO but is not a DAO itself
        let dto = CrossDerivedDto::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dto.id, TestId::default());
        assert_eq!(dto.name, TestName::default());
        assert_eq!(dto.description, TestDescription::default());
        Ok(())
    }

    // ---- Test: Dao attribute merging (child overrides base) -----------------

    #[ta_objects::dxo]
    mod dxo_dao_merge {
        use super::*;

        #[ta_objects::Dao]
        #[dao(
            sql_table = "base_merge",
            versioned(pk = "id", version_by = "version", partition_by = "id")
        )]
        pub struct BaseMergeDao {
            pub id: TestId,
            pub name: TestName,
            pub version: TestVersion,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "derived_merge")]
        #[inherits(BaseMergeDao)]
        pub struct DerivedMergeDao {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dao_merge_child_overrides_table() {
        // Child's sql_table should override base's
        let table = DerivedMergeDao::table();
        assert_eq!(table.quoted(), "derived_merge");
    }

    #[test]
    fn test_dao_merge_inherits_versioned() {
        // The child should inherit the versioned attribute from the base
        let partition = DerivedMergeDao::partition_col();
        assert_eq!(partition.quoted(), "id");

        let version = DerivedMergeDao::version_order_col();
        assert_eq!(version.quoted(), "version");
    }

    // ---- Test: field ordering is preserved ----------------------------------

    #[ta_objects::dxo]
    mod dxo_field_order {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "order_base")]
        pub struct OrderBase {
            pub first: TestId,
            pub second: TestName,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "order_derived")]
        #[inherits(OrderBase)]
        pub struct OrderDerived {
            pub third: TestDescription,
        }
    }

    #[test]
    fn test_field_order_base_before_derived() {
        // Fields should be: first, second (from base), third (own)
        let cols: Vec<_> = OrderDerived::columns().into_iter().collect();
        let col_names: Vec<_> = cols.iter().map(|c| c.quoted()).collect();

        assert_eq!(
            col_names,
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string()
            ]
        );
    }

    // ---- Test: derived field overrides base field ---------------------------

    #[ta_objects::dxo]
    mod dxo_field_override {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "override_base")]
        pub struct OverrideBase {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "override_derived")]
        #[inherits(OverrideBase)]
        pub struct OverrideDerived {
            pub name: TestName,
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_derived_field_overrides_base() -> Result<(), td_error::TdError> {
        // Derived redefines `name` — should still build with the right fields
        let dao = OverrideDerived::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dao.id, TestId::default());
        assert_eq!(dao.name, TestName::default());
        assert_eq!(dao.description, TestDescription::default());
        Ok(())
    }

    #[test]
    fn test_override_preserves_field_count() {
        // Even though `name` is declared in both base and derived, it should
        // appear only once in the final struct
        let cols: Vec<_> = OverrideDerived::columns().into_iter().collect();
        assert_eq!(cols.len(), 3); // id, name, description
    }

    // ---- Test: chained inheritance (A → B → C) ------------------------------

    #[ta_objects::dxo]
    mod dxo_chain {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "chain_a")]
        pub struct ChainA {
            pub id: TestId,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "chain_b")]
        #[inherits(ChainA)]
        pub struct ChainB {
            pub name: TestName,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "chain_c")]
        #[inherits(ChainB)]
        pub struct ChainC {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_chained_inheritance() {
        // ChainC should have fields from A, B, and C
        let cols: Vec<_> = ChainC::columns().into_iter().collect();
        let col_names: Vec<_> = cols.iter().map(|c| c.quoted()).collect();

        assert_eq!(
            col_names,
            vec![
                "id".to_string(),
                "name".to_string(),
                "description".to_string()
            ]
        );
    }

    #[test]
    fn test_chained_inheritance_buildable() -> Result<(), td_error::TdError> {
        let c = ChainC::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(c.id, TestId::default());
        assert_eq!(c.name, TestName::default());
        assert_eq!(c.description, TestDescription::default());
        Ok(())
    }

    // ---- Test: Dao → Dlo cross-type inheritance -----------------------------

    #[ta_objects::dxo]
    mod dxo_dao_to_dlo {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "dao_for_dlo")]
        pub struct DaoForDlo {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dlo]
        #[inherits(DaoForDlo)]
        pub struct DloFromDao {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dao_to_dlo_inherits_fields() -> Result<(), td_error::TdError> {
        let dlo = DloFromDao::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dlo.id, TestId::default());
        assert_eq!(dlo.name, TestName::default());
        assert_eq!(dlo.description, TestDescription::default());
        Ok(())
    }

    // ---- Test: Dlo → Dlo same-type inheritance ------------------------------

    #[ta_objects::dxo]
    mod dxo_dlo_to_dlo {
        use super::*;

        #[ta_objects::Dlo]
        pub struct BaseDlo {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dlo]
        #[inherits(BaseDlo)]
        pub struct DerivedDlo {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dlo_to_dlo_inherits_fields() -> Result<(), td_error::TdError> {
        let dlo = DerivedDlo::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dlo.id, TestId::default());
        assert_eq!(dlo.name, TestName::default());
        assert_eq!(dlo.description, TestDescription::default());
        Ok(())
    }

    // ---- Test: Dto → Dto same-type inheritance ------------------------------

    #[ta_objects::dxo]
    mod dxo_dto_to_dto {
        use super::*;

        #[ta_objects::Dto]
        pub struct BaseDto {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dto]
        #[inherits(BaseDto)]
        pub struct DerivedDto {
            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dto_to_dto_inherits_fields() -> Result<(), td_error::TdError> {
        let dto = DerivedDto::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dto.id, TestId::default());
        assert_eq!(dto.name, TestName::default());
        assert_eq!(dto.description, TestDescription::default());
        Ok(())
    }

    #[test]
    fn test_dto_serialization_with_inheritance() {
        let dto = DerivedDto::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()
            .unwrap();

        // DTOs derive Serialize/Deserialize — verify it round-trips
        let json = serde_json::to_string(&dto).unwrap();
        let deserialized: DerivedDto = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, dto.id);
        assert_eq!(deserialized.name, dto.name);
        assert_eq!(deserialized.description, dto.description);
    }

    // ---- Test: Dlo with #[exclude] ------------------------------------------

    #[ta_objects::dxo]
    mod dxo_dlo_exclude {
        use super::*;

        #[ta_objects::Dlo]
        pub struct DloWithExtra {
            pub id: TestId,
            pub name: TestName,
            pub version: TestVersion,
        }

        #[ta_objects::Dlo]
        #[inherits(DloWithExtra)]
        pub struct DloExcluding {
            #[exclude]
            pub version: TestVersion,

            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dlo_exclude_skips_field() -> Result<(), td_error::TdError> {
        // DloExcluding should have id, name, description — NOT version
        let dlo = DloExcluding::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dlo.id, TestId::default());
        assert_eq!(dlo.name, TestName::default());
        assert_eq!(dlo.description, TestDescription::default());
        Ok(())
    }

    // ---- Test: Dao → Dao → Dto chained cross-type (real-world pattern) ------

    #[ta_objects::dxo]
    mod dxo_chain_cross_type {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "chain_dao_base")]
        pub struct ChainDaoBase {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dao]
        #[dao(sql_table = "chain_dao_with_names")]
        #[inherits(ChainDaoBase)]
        pub struct ChainDaoWithNames {
            pub description: TestDescription,
        }

        #[ta_objects::Dto]
        #[inherits(ChainDaoWithNames)]
        pub struct ChainDto {
            pub version: TestVersion,
        }
    }

    #[test]
    fn test_dao_dao_dto_chain() -> Result<(), td_error::TdError> {
        // ChainDto should have: id, name (from base DAO), description (from
        // intermediate DAO), version (own)
        let dto = ChainDto::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .version(TestVersion::default())
            .build()?;

        assert_eq!(dto.id, TestId::default());
        assert_eq!(dto.name, TestName::default());
        assert_eq!(dto.description, TestDescription::default());
        assert_eq!(dto.version, TestVersion::default());
        Ok(())
    }

    // ---- Test: Dto with list(on = Dao) + inheritance ------------------------

    #[ta_objects::dxo]
    mod dxo_dto_list {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "list_base")]
        pub struct ListBaseDao {
            pub id: TestId,
            pub name: TestName,
        }

        #[ta_objects::Dto]
        #[dto(list(on = ListBaseDao))]
        #[inherits(ListBaseDao)]
        pub struct ListDto {
            #[dto(list(pagination_by = "+", filter, order_by))]
            pub id: TestId,
            #[dto(list(filter, filter_like))]
            pub name: TestName,
        }
    }

    #[test]
    fn test_dto_list_with_inheritance() -> Result<(), td_error::TdError> {
        use crate::dxo::ListDto as ListDtoTrait;

        // Verify builder works
        let dto = ListDto::builder()
            .id(TestId::default())
            .name(TestName::default())
            .build()?;

        assert_eq!(dto.id, TestId::default());
        assert_eq!(dto.name, TestName::default());

        // Verify ListDto trait — filter_col resolves correctly
        let id_col = ListDto::filter_col("id");
        assert!(id_col.is_some());

        let name_col = ListDto::filter_col("name");
        assert!(name_col.is_some());

        let unknown = ListDto::filter_col("nonexistent");
        assert!(unknown.is_none());

        // Verify filter_like_col
        let name_like = ListDto::filter_like_col("name");
        assert!(name_like.is_some());

        let id_like = ListDto::filter_like_col("id");
        assert!(id_like.is_none());

        // Verify order_by_col
        let id_order = ListDto::order_by_col("id");
        assert!(id_order.is_some());

        // Verify pagination
        let (pag_col, pag_order) = ListDto::pagination_col();
        assert_eq!(pag_col.quoted(), "id");
        assert!(matches!(pag_order, sea_query::Order::Asc));

        Ok(())
    }

    // ---- Test: Dto with #[exclude] from Dao ---------------------------------

    #[ta_objects::dxo]
    mod dxo_dto_exclude {
        use super::*;

        #[ta_objects::Dao]
        #[dao(sql_table = "dto_excl_base")]
        pub struct DtoExclBase {
            pub id: TestId,
            pub name: TestName,
            pub version: TestVersion,
        }

        #[ta_objects::Dto]
        #[inherits(DtoExclBase)]
        pub struct DtoExcluding {
            #[exclude]
            pub version: TestVersion,

            pub description: TestDescription,
        }
    }

    #[test]
    fn test_dto_exclude_from_dao() -> Result<(), td_error::TdError> {
        // DtoExcluding should have id, name, description — NOT version
        let dto = DtoExcluding::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()?;

        assert_eq!(dto.id, TestId::default());
        assert_eq!(dto.name, TestName::default());
        assert_eq!(dto.description, TestDescription::default());
        Ok(())
    }

    #[test]
    fn test_dto_exclude_serialization() {
        // Excluded field should not appear in serialized output
        let dto = DtoExcluding::builder()
            .id(TestId::default())
            .name(TestName::default())
            .description(TestDescription::default())
            .build()
            .unwrap();

        let json: serde_json::Value = serde_json::to_value(&dto).unwrap();
        assert!(json.get("id").is_some());
        assert!(json.get("name").is_some());
        assert!(json.get("description").is_some());
        assert!(json.get("version").is_none());
    }

    #[ta_objects::dxo]
    mod dxo_tristate {
        use super::*;

        #[ta_objects::Dto]
        pub struct TriStateUpdate {
            #[builder(default)]
            pub value: Option<Option<TestName>>,
        }
    }

    /// The Dto macro auto-injects the `double_option` deserializer for any
    /// `Option<Option<T>>` field, so JSON distinguishes absent (unchanged),
    /// `null` (clear), and a value (set).
    #[test]
    fn test_dto_auto_injects_double_option_tristate() {
        use dxo_tristate::TriStateUpdate;

        let unchanged: TriStateUpdate = serde_json::from_str("{}").unwrap();
        assert_eq!(unchanged.value, None);

        let cleared: TriStateUpdate = serde_json::from_str(r#"{"value":null}"#).unwrap();
        assert_eq!(cleared.value, Some(None));

        let set: TriStateUpdate = serde_json::from_str(r#"{"value":"hello"}"#).unwrap();
        assert_eq!(set.value, Some(Some(TestName::try_from("hello").unwrap())));
    }
}
