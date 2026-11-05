//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use arrow::array::{
    Array, BinaryViewArray, DictionaryArray, Int64Array, LargeListArray, StringArray,
    StringViewArray, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{Int32Type, TimeUnit};
use arrow::record_batch::RecordBatchIterator;
use arrow_avro::reader::ReaderBuilder;
use std::io::Cursor;

fn source(batches: Vec<RecordBatch>, schema: SchemaRef) -> impl RecordBatchReader {
    RecordBatchIterator::new(batches.into_iter().map(Ok), schema)
}

fn read_back(bytes: Vec<u8>) -> Vec<RecordBatch> {
    ReaderBuilder::new()
        .build(Cursor::new(bytes))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn polars_like() -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Utf8View, true));
    let offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 2, 2, 3].into());
    let tags = LargeListArray::new(
        item.clone(),
        offsets,
        Arc::new(StringViewArray::from(vec!["a", "b", "c"])),
        None,
    );
    let point = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Int64, true)),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
    )]);
    let kind: DictionaryArray<Int32Type> = vec!["red", "blue", "red"].into_iter().collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8View, true),
        Field::new("blob", DataType::BinaryView, true),
        Field::new("tags", DataType::LargeList(item), true),
        Field::new("point", point.data_type().clone(), true),
        Field::new("kind", kind.data_type().clone(), true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(vec![Some("ann"), None, Some("cy")])),
            Arc::new(BinaryViewArray::from(vec![
                Some(b"x".as_slice()),
                Some(b"".as_slice()),
                None,
            ])),
            Arc::new(tags),
            Arc::new(point),
            Arc::new(kind),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(0), Some(1_000_000), None])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap()
}

#[test]
fn view_large_and_dictionary_types_are_written_in_their_plain_form() {
    let batch = polars_like();
    let mut bytes = Vec::new();
    let rows = written(source(vec![batch.clone()], batch.schema()), &mut bytes).unwrap();
    assert_eq!(rows, 3);
    let [back] = read_back(bytes).try_into().unwrap();
    let names = back
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        names.iter().collect::<Vec<_>>(),
        vec![Some("ann"), None, Some("cy")]
    );
    let kinds = back.column_by_name("kind").unwrap();
    let kinds = cast(kinds, &DataType::Utf8).unwrap();
    let kinds = kinds.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        kinds.iter().collect::<Vec<_>>(),
        vec![Some("red"), Some("blue"), Some("red")]
    );
    assert!(matches!(
        back.column_by_name("tags").unwrap().data_type(),
        DataType::List(_)
    ));
    assert_eq!(back.column_by_name("at").unwrap().null_count(), 1);
}

#[test]
fn every_batch_is_written() {
    let batch = polars_like();
    let mut bytes = Vec::new();
    let rows = written(
        source(vec![batch.clone(), batch.clone()], batch.schema()),
        &mut bytes,
    )
    .unwrap();
    assert_eq!(rows, 6);
    let back = read_back(bytes);
    assert_eq!(back.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
}

#[test]
fn no_batch_still_writes_a_file_with_the_schema() {
    let schema = polars_like().schema();
    let mut bytes = Vec::new();
    assert_eq!(written(source(vec![], schema), &mut bytes).unwrap(), 0);
    assert!(!bytes.is_empty());
    assert!(read_back(bytes).iter().all(|batch| batch.num_rows() == 0));
}

#[test]
fn plain_reaches_every_depth() {
    let inner = Arc::new(Field::new("v", DataType::LargeUtf8, true));
    let record = DataType::Struct(Fields::from(vec![Field::new(
        "l",
        DataType::LargeList(inner),
        true,
    )]));
    let expected = DataType::Struct(Fields::from(vec![Field::new(
        "l",
        DataType::List(Arc::new(Field::new("v", DataType::Utf8, true))),
        true,
    )]));
    assert_eq!(plain(&record), expected);
    assert_eq!(plain(&DataType::Int8), DataType::Int8);
}
