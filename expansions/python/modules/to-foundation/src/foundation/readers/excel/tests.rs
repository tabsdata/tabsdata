//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[derive(Default)]
struct StoredZip {
    out: Vec<u8>,
    entries: Vec<(String, u32, u32, u32)>,
}

impl StoredZip {
    fn add(&mut self, name: &str, data: &[u8]) {
        let offset = self.out.len() as u32;
        let crc = crc32(data);
        let size = data.len() as u32;
        self.out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        for field in [20u16, 0, 0, 0, 0] {
            self.out.extend_from_slice(&field.to_le_bytes());
        }
        self.out.extend_from_slice(&crc.to_le_bytes());
        self.out.extend_from_slice(&size.to_le_bytes());
        self.out.extend_from_slice(&size.to_le_bytes());
        self.out
            .extend_from_slice(&(name.len() as u16).to_le_bytes());
        self.out.extend_from_slice(&0u16.to_le_bytes());
        self.out.extend_from_slice(name.as_bytes());
        self.out.extend_from_slice(data);
        self.entries.push((name.to_string(), crc, size, offset));
    }

    fn finish(mut self) -> Vec<u8> {
        let dir_at = self.out.len() as u32;
        for (name, crc, size, offset) in &self.entries {
            self.out.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
            for field in [20u16, 20, 0, 0, 0, 0] {
                self.out.extend_from_slice(&field.to_le_bytes());
            }
            self.out.extend_from_slice(&crc.to_le_bytes());
            self.out.extend_from_slice(&size.to_le_bytes());
            self.out.extend_from_slice(&size.to_le_bytes());
            self.out
                .extend_from_slice(&(name.len() as u16).to_le_bytes());
            for field in [0u16, 0, 0, 0] {
                self.out.extend_from_slice(&field.to_le_bytes());
            }
            self.out.extend_from_slice(&0u32.to_le_bytes());
            self.out.extend_from_slice(&offset.to_le_bytes());
            self.out.extend_from_slice(name.as_bytes());
        }
        let dir_size = self.out.len() as u32 - dir_at;
        let count = self.entries.len() as u16;
        self.out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
        for field in [0u16, 0, count, count] {
            self.out.extend_from_slice(&field.to_le_bytes());
        }
        self.out.extend_from_slice(&dir_size.to_le_bytes());
        self.out.extend_from_slice(&dir_at.to_le_bytes());
        self.out.extend_from_slice(&0u16.to_le_bytes());
        self.out
    }
}

struct Part {
    name: &'static str,
    state: &'static str,
    chart: bool,
    body: String,
}

fn sheet(name: &'static str, rows: &str) -> Part {
    Part {
        name,
        state: "visible",
        chart: false,
        body: format!("<worksheet><sheetData>{rows}</sheetData></worksheet>"),
    }
}

fn sheet_with_dimension(name: &'static str, dimension: &str, rows: &str) -> Part {
    Part {
        name,
        state: "visible",
        chart: false,
        body: format!(
            "<worksheet><dimension ref=\"{dimension}\"/>\
             <sheetData>{rows}</sheetData></worksheet>"
        ),
    }
}

fn hidden(name: &'static str, rows: &str) -> Part {
    Part {
        state: "hidden",
        ..sheet(name, rows)
    }
}

fn chart(name: &'static str) -> Part {
    Part {
        name,
        state: "visible",
        chart: true,
        body: "<chartsheet><drawing/></chartsheet>".to_string(),
    }
}

const REL: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

fn book(parts: &[Part], shared: &[&str]) -> Bytes {
    book_with(parts, shared, false)
}

fn book_with(parts: &[Part], shared: &[&str], date1904: bool) -> Bytes {
    let mut zip = StoredZip::default();
    zip.add(
        "_rels/.rels",
        format!(
            "<Relationships><Relationship Id=\"rId1\" Type=\"{REL}/officeDocument\" \
             Target=\"xl/workbook.xml\"/></Relationships>"
        )
        .as_bytes(),
    );

    let mut sheets = String::new();
    let mut rels = String::new();
    for (index, part) in parts.iter().enumerate() {
        let id = index + 1;
        let (kind, dir) = if part.chart {
            ("chartsheet", "chartsheets")
        } else {
            ("worksheet", "worksheets")
        };
        sheets.push_str(&format!(
            "<sheet name=\"{}\" sheetId=\"{id}\" state=\"{}\" r:id=\"rId{id}\"/>",
            part.name, part.state
        ));
        rels.push_str(&format!(
            "<Relationship Id=\"rId{id}\" Type=\"{REL}/{kind}\" \
             Target=\"{dir}/sheet{id}.xml\"/>"
        ));
        zip.add(&format!("xl/{dir}/sheet{id}.xml"), part.body.as_bytes());
    }
    let properties = if date1904 {
        "<workbookPr date1904=\"1\"/>"
    } else {
        ""
    };
    zip.add(
        "xl/workbook.xml",
        format!(
            "<workbook xmlns:r=\"{REL}\">{properties}\
             <sheets>{sheets}</sheets></workbook>"
        )
        .as_bytes(),
    );
    zip.add(
        "xl/_rels/workbook.xml.rels",
        format!("<Relationships>{rels}</Relationships>").as_bytes(),
    );
    zip.add(
        "xl/styles.xml",
        b"<styleSheet><cellXfs count=\"3\">\
          <xf numFmtId=\"0\"/><xf numFmtId=\"14\"/><xf numFmtId=\"46\"/>\
          </cellXfs></styleSheet>",
    );
    let entries: String = shared
        .iter()
        .map(|s| format!("<si><t>{s}</t></si>"))
        .collect();
    zip.add(
        "xl/sharedStrings.xml",
        format!("<sst uniqueCount=\"{}\">{entries}</sst>", shared.len()).as_bytes(),
    );
    Bytes::from(zip.finish())
}

fn one(rows: &str, shared: &[&str]) -> Bytes {
    book(&[sheet("Data", rows)], shared)
}

fn opts(sheet: u32) -> ExcelOptions {
    ExcelOptions {
        sheet,
        ..Default::default()
    }
}

fn read(workbook: Bytes, sheet: u32) -> (SchemaRef, Vec<RecordBatch>) {
    read_with(workbook, opts(sheet))
}

fn read_with(workbook: Bytes, options: ExcelOptions) -> (SchemaRef, Vec<RecordBatch>) {
    let (schema, batches) = decode_bytes(workbook, options, 8192).unwrap();
    (schema, batches.map(|b| b.unwrap()).collect())
}

fn types(schema: &SchemaRef) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

fn names(schema: &SchemaRef) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

fn ints(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        out.extend(column.as_any().downcast_ref::<Int64Array>().unwrap().iter());
    }
    out
}

fn floats(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        out.extend(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter(),
        );
    }
    out
}

fn stamps(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        out.extend(
            column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter(),
        );
    }
    out
}

fn texts(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(column.iter().map(|v| v.map(str::to_string)));
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn read_err(workbook: Bytes, sheet: u32) -> ExcelError {
    match decode_bytes(workbook, opts(sheet), 8192) {
        Ok(_) => panic!("expected sheet {sheet} to fail"),
        Err(e) => e,
    }
}

fn micros(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, minute, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

#[test]
fn all_ints_resolve_to_int64() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
             <row r=\"3\"><c r=\"A3\"><v>2</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(types(&schema), vec![("n".to_string(), DataType::Int64)]);
    assert_eq!(ints(&batches, "n"), vec![Some(1), Some(2)]);
}

#[test]
fn an_int_and_a_float_resolve_to_float64() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
             <row r=\"3\"><c r=\"A3\"><v>2.5</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(types(&schema), vec![("n".to_string(), DataType::Float64)]);
    assert_eq!(floats(&batches, "n"), vec![Some(1.0), Some(2.5)]);
}

#[test]
fn an_int_and_a_string_resolve_to_utf8() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
             <row r=\"3\"><c r=\"A3\" t=\"s\"><v>1</v></c></row>",
            &["n", "x"],
        ),
        1,
    );
    assert_eq!(types(&schema), vec![("n".to_string(), DataType::Utf8)]);
    assert_eq!(
        texts(&batches, "n"),
        vec![Some("1".to_string()), Some("x".to_string())]
    );
}

#[test]
fn the_drift_row_can_sit_anywhere() {
    let early = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>2.5</v></c></row>\
                 <row r=\"3\"><c r=\"A3\"><v>1</v></c></row>";
    let late = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
                <row r=\"3\"><c r=\"A3\"><v>2.5</v></c></row>";
    for body in [early, late] {
        let (schema, _) = read(one(body, &["n"]), 1);
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    }
}

#[test]
fn an_int_and_a_date_do_not_coerce_and_fall_to_utf8() {
    let (schema, _) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
             <row r=\"3\"><c r=\"A3\" s=\"1\"><v>45943.5</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn empty_cells_contribute_no_evidence() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\" s=\"0\"/></row>\
             <row r=\"3\"><c r=\"A3\" s=\"0\"/></row>\
             <row r=\"4\"><c r=\"A4\" t=\"s\"><v>1</v></c></row>",
            &["n", "late"],
        ),
        1,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        texts(&batches, "n"),
        vec![None, None, Some("late".to_string())]
    );
}

#[test]
fn a_column_with_no_evidence_at_all_is_utf8() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
              <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\" s=\"0\"/></row>",
            &["n", "blank"],
        ),
        1,
    );
    assert_eq!(
        types(&schema),
        vec![
            ("n".to_string(), DataType::Int64),
            ("blank".to_string(), DataType::Utf8),
        ]
    );
    assert_eq!(texts(&batches, "blank"), vec![None]);
}

#[test]
fn zero_padded_codes_keep_their_digits() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\" t=\"s\"><v>1</v></c></row>\
             <row r=\"3\"><c r=\"A3\" t=\"s\"><v>2</v></c></row>",
            &["code", "00731", "00042"],
        ),
        1,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        texts(&batches, "code"),
        vec![Some("00731".to_string()), Some("00042".to_string())]
    );
}

#[test]
fn date_formatted_numerics_are_timestamps_not_floats() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\" s=\"1\"><v>45943.5</v></c></row>\
             <row r=\"3\"><c r=\"A3\" s=\"1\"/></row>",
            &["when"],
        ),
        1,
    );
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        stamps(&batches, "when"),
        vec![Some(micros(2025, 10, 13, 12, 0)), None]
    );
}

#[test]
fn an_iso_date_cell_is_a_timestamp() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\" t=\"d\"><v>2026-09-08T06:30:00</v></c></row>",
            &["when"],
        ),
        1,
    );
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        stamps(&batches, "when"),
        vec![Some(micros(2026, 9, 8, 6, 30))]
    );
}

#[test]
fn a_duration_formatted_cell_is_elapsed_days_not_an_instant() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\" s=\"2\"><v>0.5</v></c></row>",
            &["took"],
        ),
        1,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    assert_eq!(floats(&batches, "took"), vec![Some(0.5)]);
}

#[test]
fn booleans_and_errors_are_text() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
              <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
             <row r=\"2\"><c r=\"A2\" t=\"b\"><v>1</v></c>\
              <c r=\"B2\" t=\"e\"><v>#N/A</v></c></row>",
            &["flag", "bad"],
        ),
        1,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(texts(&batches, "flag"), vec![Some("true".to_string())]);
    assert_eq!(texts(&batches, "bad"), vec![Some("#N/A".to_string())]);
}

#[test]
fn an_inline_string_reads_like_a_shared_one() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>name</t></is></c></row>\
             <row r=\"2\"><c r=\"A2\" t=\"inlineStr\"><is><t>alice</t></is></c></row>",
            &[],
        ),
        1,
    );
    assert_eq!(types(&schema), vec![("name".to_string(), DataType::Utf8)]);
    assert_eq!(texts(&batches, "name"), vec![Some("alice".to_string())]);
}

fn headerless() -> ExcelOptions {
    ExcelOptions {
        sheet: 1,
        has_header: false,
    }
}

#[test]
fn with_no_header_the_first_row_is_data() {
    let body = "<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>\
                <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>";
    let (schema, batches) = read(one(body, &[]), 1);
    assert_eq!(schema.field(0).name(), "1");
    assert_eq!(ints(&batches, "1"), vec![Some(2)]);
    let (schema, batches) = read_with(one(body, &[]), headerless());
    assert_eq!(
        types(&schema),
        vec![("column_1".to_string(), DataType::Int64)]
    );
    assert_eq!(ints(&batches, "column_1"), vec![Some(1), Some(2)]);
}

#[test]
fn with_no_header_every_row_votes_on_the_type() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>";
    let (schema, batches) = read_with(one(body, &["n/a"]), headerless());
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        texts(&batches, "column_1"),
        vec![Some("n/a".to_string()), Some("2".to_string())]
    );
}

#[test]
fn with_no_header_columns_are_numbered_across_the_whole_width() {
    let body = "<row r=\"1\"><c r=\"A1\"><v>1</v></c>\
                 <c r=\"B1\" t=\"s\"><v>0</v></c><c r=\"C1\"><v>1.5</v></c></row>";
    let (schema, _) = read_with(one(body, &["x"]), headerless());
    assert_eq!(
        types(&schema),
        vec![
            ("column_1".to_string(), DataType::Int64),
            ("column_2".to_string(), DataType::Utf8),
            ("column_3".to_string(), DataType::Float64),
        ]
    );
}

#[test]
fn with_no_header_leading_blank_rows_are_not_emitted() {
    let body = "<row r=\"4\"><c r=\"A4\"><v>1</v></c></row>\
                <row r=\"6\"><c r=\"A6\"><v>3</v></c></row>";
    let (_schema, batches) = read_with(one(body, &[]), headerless());
    assert_eq!(ints(&batches, "column_1"), vec![Some(1), None, Some(3)]);
}

#[test]
fn has_header_defaults_to_on() {
    assert!(ExcelOptions::default().has_header);
    assert!(opts(1).has_header);
}

#[test]
fn an_empty_sheet_is_the_same_either_way() {
    for options in [opts(1), headerless()] {
        let (schema, batches) = read_with(one("", &[]), options);
        assert_eq!(
            types(&schema),
            vec![("column_1".to_string(), DataType::Utf8)]
        );
        assert!(batches.is_empty());
    }
}

#[test]
fn the_integer_ladder_stops_where_f64_stops_counting() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                 <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                <row r=\"2\"><c r=\"A2\"><v>9007199254740991</v></c>\
                 <c r=\"B2\"><v>9007199254740992</v></c></row>";
    let (schema, batches) = read(one(body, &["exact", "past"]), 1);
    assert_eq!(
        types(&schema),
        vec![
            ("exact".to_string(), DataType::Int64),
            ("past".to_string(), DataType::Float64),
        ]
    );
    assert_eq!(ints(&batches, "exact"), vec![Some(9_007_199_254_740_991)]);
    assert_eq!(
        floats(&batches, "past"),
        vec![Some(9_007_199_254_740_992.0)]
    );
}

#[test]
fn a_negative_whole_number_is_still_an_int() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\"><v>-7</v></c></row>";
    let (schema, batches) = read(one(body, &["n"]), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(ints(&batches, "n"), vec![Some(-7)]);
}

#[test]
fn a_float_and_a_date_do_not_coerce_either() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\"><v>2.5</v></c></row>\
                <row r=\"3\"><c r=\"A3\" s=\"1\"><v>45943.5</v></c></row>";
    let (schema, _) = read(one(body, &["n"]), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn two_dates_stay_a_timestamp() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\" s=\"1\"><v>45943.5</v></c></row>\
                <row r=\"3\"><c r=\"A3\" s=\"1\"><v>45944.5</v></c></row>";
    let (schema, batches) = read(one(body, &["when"]), 1);
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        stamps(&batches, "when"),
        vec![
            Some(micros(2025, 10, 13, 12, 0)),
            Some(micros(2025, 10, 14, 12, 0)),
        ]
    );
}

#[test]
fn a_1904_workbook_shifts_every_serial_by_1462_days() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\" s=\"1\"><v>45943.5</v></c></row>";
    let epoch_1900 = read(book(&[sheet("Data", body)], &["when"]), 1);
    let epoch_1904 = read(book_with(&[sheet("Data", body)], &["when"], true), 1);

    let day = 24 * 60 * 60 * 1_000_000i64;
    assert_eq!(
        stamps(&epoch_1900.1, "when"),
        vec![Some(micros(2025, 10, 13, 12, 0))]
    );
    assert_eq!(
        stamps(&epoch_1904.1, "when"),
        vec![Some(micros(2025, 10, 13, 12, 0) + 1462 * day)]
    );
}

#[test]
fn the_1900_leap_year_bug_reads_as_null_not_a_wrong_date() {
    let body = "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                <row r=\"2\"><c r=\"A2\" s=\"1\"><v>60</v></c></row>\
                <row r=\"3\"><c r=\"A3\" s=\"1\"><v>61</v></c></row>";
    let (schema, batches) = read(one(body, &["when"]), 1);
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let read_back = stamps(&batches, "when");
    assert_eq!(read_back[0], None, "1900-02-29 has no instant");
    assert_eq!(read_back[1], Some(micros(1900, 3, 1, 0, 0)));
}

#[test]
fn a_cell_with_no_r_attribute_lands_in_the_running_column() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
              <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
             <row r=\"2\"><c><v>7</v></c><c><v>8</v></c></row>",
            &["a", "b"],
        ),
        1,
    );
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(ints(&batches, "a"), vec![Some(7)]);
    assert_eq!(ints(&batches, "b"), vec![Some(8)]);
}

#[test]
fn ragged_trailing_cells_become_nulls_without_shifting_columns() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
              <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\"><v>2</v></c></row>\
             <row r=\"3\"><c r=\"A3\"><v>3</v></c></row>\
             <row r=\"4\"><c r=\"B4\"><v>4</v></c></row>",
            &["a", "b"],
        ),
        1,
    );
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(ints(&batches, "a"), vec![Some(1), Some(3), None]);
    assert_eq!(ints(&batches, "b"), vec![Some(2), None, Some(4)]);
}

#[test]
fn an_under_reporting_dimension_does_not_narrow_the_schema() {
    let (schema, batches) = read(
        book(
            &[sheet_with_dimension(
                "Data",
                "A1:A2",
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
                  <c r=\"B1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\"><v>2</v></c></row>",
            )],
            &["a", "b"],
        ),
        1,
    );
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(ints(&batches, "b"), vec![Some(2)]);
}

#[test]
fn an_over_reporting_dimension_does_not_widen_the_schema() {
    let (schema, _) = read(
        book(
            &[sheet_with_dimension(
                "Data",
                "A1:XFD1048576",
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
            )],
            &["a"],
        ),
        1,
    );
    assert_eq!(schema.fields().len(), 1);
}

#[test]
fn an_interior_blank_row_keeps_its_place() {
    let (_schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>\
             <row r=\"5\"><c r=\"A5\"><v>4</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(
        ints(&batches, "n"),
        vec![Some(1), None, None, Some(4)],
        "rows 3 and 4 are blank, not absent"
    );
}

#[test]
fn a_gap_before_the_first_data_row_is_kept_too() {
    let (_schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
             <row r=\"3\"><c r=\"A3\"><v>1</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(ints(&batches, "n"), vec![None, Some(1)]);
}

#[test]
fn there_is_no_trailing_run_of_phantom_rows() {
    let (_schema, batches) = read(
        book(
            &[sheet_with_dimension(
                "Data",
                "A1:A1048576",
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
            )],
            &["n"],
        ),
        1,
    );
    assert_eq!(total_rows(&batches), 1);
}

#[test]
fn the_first_non_empty_row_names_the_columns() {
    let (schema, batches) = read(
        one(
            "<row r=\"4\"><c r=\"A4\" t=\"s\"><v>0</v></c></row>\
             <row r=\"5\"><c r=\"A5\"><v>1</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(types(&schema), vec![("n".to_string(), DataType::Int64)]);
    assert_eq!(ints(&batches, "n"), vec![Some(1)]);
}

#[test]
fn blank_and_repeated_headers_get_usable_names() {
    let (schema, _) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c>\
              <c r=\"B1\" s=\"0\"/><c r=\"C1\" t=\"s\"><v>0</v></c>\
              <c r=\"D1\" t=\"s\"><v>1</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
            &["id", "   "],
        ),
        1,
    );
    assert_eq!(names(&schema), vec!["id", "column_2", "id_2", "column_4"]);
}

#[test]
fn a_numeric_header_becomes_its_own_text() {
    let (schema, _) = read(
        one(
            "<row r=\"1\"><c r=\"A1\"><v>2026</v></c></row>\
             <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
            &[],
        ),
        1,
    );
    assert_eq!(schema.field(0).name(), "2026");
}

#[test]
fn numbering_counts_hidden_sheets() {
    let workbook = book(
        &[
            hidden(
                "Config",
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
            ),
            sheet(
                "Data",
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>1</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>2</v></c></row>",
            ),
        ],
        &["setting", "value"],
    );
    assert_eq!(read(workbook.clone(), 1).0.field(0).name(), "setting");
    assert_eq!(read(workbook, 2).0.field(0).name(), "value");
}

#[test]
fn a_chart_sheet_occupies_an_index_but_cannot_be_read() {
    let workbook = book(
        &[
            chart("Q3 chart"),
            sheet(
                "Data",
                "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>\
                 <row r=\"2\"><c r=\"A2\"><v>1</v></c></row>",
            ),
        ],
        &["n"],
    );
    let err = read_err(workbook.clone(), 1);
    assert!(
        matches!(err, ExcelError::NotAWorksheet { number: 1, .. }),
        "{err}"
    );
    assert_eq!(read(workbook, 2).0.field(0).name(), "n");
}

#[test]
fn sheet_zero_and_out_of_range_are_distinct_errors() {
    let workbook = one("<row r=\"1\"><c r=\"A1\"><v>1</v></c></row>", &[]);
    assert!(matches!(
        read_err(workbook.clone(), 0),
        ExcelError::SheetNumber(0)
    ));
    let err = read_err(workbook, 99);
    assert!(
        matches!(
            err,
            ExcelError::SheetOutOfRange {
                requested: 99,
                available: 1
            }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("1 sheet(s)"), "{err}");
}

#[test]
fn an_empty_sheet_has_one_text_column_and_no_rows() {
    let (schema, batches) = read(one("", &[]), 1);
    assert_eq!(
        types(&schema),
        vec![("column_1".to_string(), DataType::Utf8)]
    );
    assert!(batches.is_empty());
}

#[test]
fn a_header_with_no_data_rows_keeps_its_schema() {
    let (schema, batches) = read(
        one(
            "<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>",
            &["n"],
        ),
        1,
    );
    assert_eq!(types(&schema), vec![("n".to_string(), DataType::Utf8)]);
    assert!(batches.is_empty());
}

fn tall_sheet(data_rows: usize) -> Bytes {
    let mut body = String::from("<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>");
    for row in 2..=data_rows + 1 {
        body.push_str(&format!(
            "<row r=\"{row}\"><c r=\"A{row}\"><v>{}</v></c></row>",
            row - 1
        ));
    }
    one(&body, &["n"])
}

#[test]
fn rows_are_cut_into_batches_of_the_requested_size() {
    let (_schema, batches) = decode_bytes(tall_sheet(1_000), opts(1), 100).unwrap();
    let sizes: Vec<usize> = batches.map(|b| b.unwrap().num_rows()).collect();
    assert_eq!(sizes.len(), 10);
    assert!(sizes.iter().all(|n| *n == 100), "{sizes:?}");
}

#[test]
fn a_consumer_that_stops_early_stops_the_walk() {
    let mut book = open(Cursor::new(tall_sheet(1_000))).unwrap();
    let resolved = resolve(&mut book, opts(1)).unwrap();
    let mut taken = 0;
    stream(&mut book, &resolved, 100, |_batch| {
        taken += 1;
        false
    })
    .unwrap();
    assert_eq!(taken, 1);
}

#[test]
fn batches_reach_the_sink_before_the_walk_ends() {
    let mut narrow = String::from("<row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c></row>");
    for row in 2..=1_001 {
        narrow.push_str(&format!(
            "<row r=\"{row}\"><c r=\"A{row}\"><v>{}</v></c></row>",
            row - 1
        ));
    }
    let wide = narrow.replace(
        "<row r=\"502\"><c r=\"A502\"><v>501</v></c></row>",
        "<row r=\"502\"><c r=\"A502\"><v>501</v></c><c r=\"B502\"><v>9</v></c></row>",
    );
    assert_ne!(wide, narrow, "the tripwire row was not substituted");

    let resolved = resolve(
        &mut open(Cursor::new(one(&narrow, &["n"]))).unwrap(),
        opts(1),
    )
    .unwrap();
    assert_eq!(resolved.verdicts.len(), 1);

    let mut book = open(Cursor::new(one(&wide, &["n"]))).unwrap();
    let mut taken = 0;
    let err = stream(&mut book, &resolved, 100, |_batch| {
        taken += 1;
        true
    })
    .unwrap_err();

    assert!(err.to_string().contains("changed between passes"), "{err}");
    assert_eq!(taken, 5, "batches must reach the sink as the walk proceeds");
}

#[test]
fn a_final_short_batch_is_still_emitted() {
    let (_schema, batches) = decode_bytes(tall_sheet(250), opts(1), 100).unwrap();
    let sizes: Vec<usize> = batches.map(|b| b.unwrap().num_rows()).collect();
    assert_eq!(sizes, vec![100, 100, 50]);
}

#[test]
fn a_workbook_that_is_not_a_zip_fails_as_an_excel_error() {
    let err = read_err(Bytes::from_static(b"id,name\n1,alice\n"), 1);
    assert!(matches!(err, ExcelError::Xlsx(_)), "{err}");
}
