//
//  Copyright 2024 Tabs Data Inc.
//

//noinspection RsUnresolvedPath
#[cfg(test)]
mod tests {
    use derive_builder::Builder;
    use getset::Getters;
    use td_concrete::concrete;

    #[derive(Debug, Clone, Builder, Getters)]
    #[getset(get = "pub")]
    struct TestStruct<T, S> {
        field1: T,
        field2: Option<S>,
    }

    #[concrete(root_dir = "facets/td-concrete/tests")]
    type ConcreteTestStruct = TestStruct<i32, String>;

    #[test]
    fn test_generic_from_concrete_in_struct() {
        let concrete = ConcreteTestStructBuilder::default()
            .field1(1)
            .field2(Some("test".to_string()))
            .build()
            .unwrap();
        let generic: TestStruct<i32, String> = concrete.clone().into();

        assert_eq!(generic.field1(), concrete.field1());
        assert_eq!(generic.field2(), concrete.field2());
    }

    #[test]
    fn test_concrete_from_generic_in_struct() {
        let generic = TestStructBuilder::default()
            .field1(2)
            .field2(Some("test_2".to_string()))
            .build()
            .unwrap();
        let concrete: ConcreteTestStruct = generic.clone().into();

        assert_eq!(concrete.field1(), generic.field1());
        assert_eq!(concrete.field2(), generic.field2());
    }

    #[derive(Debug, Clone)]
    enum TestEnum<T> {
        Variant1(T),
        Variant2(Option<T>),
    }

    #[concrete(root_dir = "facets/td-concrete/tests")]
    type ConcreteTestEnum = TestEnum<i32>;

    #[test]
    fn test_generic_from_concrete_in_enum() {
        let concrete_1 = ConcreteTestEnum::Variant1(10);
        let generic_1: TestEnum<i32> = concrete_1.clone().into();
        let concrete_2 = ConcreteTestEnum::Variant2(Some(20));
        let generic_2: TestEnum<i32> = concrete_2.clone().into();

        assert!(matches!(concrete_1, ConcreteTestEnum::Variant1(10)));
        assert!(matches!(generic_1, TestEnum::Variant1(10)));
        assert!(matches!(concrete_2, ConcreteTestEnum::Variant2(Some(20))));
        assert!(matches!(generic_2, TestEnum::Variant2(Some(20))));
    }

    #[test]
    fn test_concrete_from_generic_in_enum() {
        let generic_1 = TestEnum::Variant1(30);
        let concrete_1: ConcreteTestEnum = generic_1.clone().into();
        let generic_2 = TestEnum::Variant2(Some(40));
        let concrete_2: ConcreteTestEnum = generic_2.clone().into();

        assert!(matches!(generic_1, TestEnum::Variant1(30)));
        assert!(matches!(concrete_1, ConcreteTestEnum::Variant1(30)));
        assert!(matches!(generic_2, TestEnum::Variant2(Some(40))));
        assert!(matches!(concrete_2, ConcreteTestEnum::Variant2(Some(40))));
    }

    #[derive(Debug, Clone, Builder, Getters)]
    #[getset(get = "pub")]
    struct TestStructWithEnumGeneric<T, S> {
        field1: T,
        field2: Option<S>,
    }

    type TestStructWithEnum = TestStructWithEnumGeneric<TestEnum<i32>, TestEnum<i32>>;

    #[concrete(into = TestStructWithEnum, root_dir = "facets/td-concrete/tests")]
    type ConcreteTestStructWithEnum = TestStructWithEnumGeneric<ConcreteTestEnum, ConcreteTestEnum>;

    #[test]
    fn test_additional_into() {
        let concrete = ConcreteTestStructWithEnumBuilder::default()
            .field1(ConcreteTestEnum::Variant2(None))
            .field2(Some(ConcreteTestEnum::Variant1(7)))
            .build()
            .unwrap();

        let generic: TestStructWithEnum = concrete.clone().into();

        assert!(matches!(generic.field1(), TestEnum::Variant2(None)));
        assert!(matches!(generic.field2(), Some(TestEnum::Variant1(7))));
    }
}
