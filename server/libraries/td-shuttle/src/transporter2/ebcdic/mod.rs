//
// Copyright 2026 Tabsdata Inc.
//

//! EBCDIC fixed-length records described by a COBOL copybook — read only.
//!
//! The copybook rides inline on the format options, so the schema is a pure
//! function of it and this reader touches nothing but the data file.
//!
//! Records are fixed length, which makes `size % lrecl != 0` decisive: the
//! wrong copybook version, or a text-mode transfer. Checked before decoding.
//!
//! A mainframe extract has no conventional extension, so `AUTO` never resolves
//! here and the format must be named explicitly.

pub mod builders;
pub mod codepage;
pub mod copybook;
pub mod value;

use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::format::BatchIter;
use crate::transporter2::spec::EbcdicOptions;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use builders::RecordDecoder;
use bytes::Bytes;
use copybook::Layout;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex};

/// Fixed, not configurable: the wire carries a copybook and a code page only.
/// `FILLER` occupies its bytes without becoming a column, trailing spaces are
/// padding rather than value, and z/OS emits hex float unless told otherwise.
const KEEP_FILLER: bool = false;
const HFP: bool = true;
const TRIM: bool = true;

/// One source often names the same layout on every path; parse each once.
static LAYOUTS: LazyLock<Mutex<HashMap<String, Arc<Layout>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Parse `copybook`, or hand back the layout already parsed from it. Callers
/// use it for its errors too: parsing up front stops a staged convert
/// downloading a whole file before it finds a typo.
pub fn layout_of(copybook: &str) -> Result<Arc<Layout>> {
    if let Some(hit) = LAYOUTS.lock().expect("layout cache poisoned").get(copybook) {
        return Ok(hit.clone());
    }
    let layout = Arc::new(copybook::parse_copybook(copybook, KEEP_FILLER, HFP)?);
    LAYOUTS
        .lock()
        .expect("layout cache poisoned")
        .insert(copybook.to_string(), layout.clone());
    Ok(layout)
}

/// The schema a copybook decodes to. Reads no data.
pub fn schema(options: &EbcdicOptions) -> Result<SchemaRef> {
    builders::schema_of(layout_of(&options.copybook)?.as_ref())
}

/// A remainder is this reader's most informative error: between them, the
/// wrong copybook and a text-mode transfer are most failures.
fn check_length(size: u64, lrecl: usize) -> Result<()> {
    if lrecl == 0 {
        return Err(TransporterError::Copybook(
            "copybook describes a zero-length record".to_string(),
        ));
    }
    let remainder = size % lrecl as u64;
    if remainder != 0 {
        return Err(TransporterError::EbcdicRecordLengthMismatch {
            size,
            lrecl,
            remainder,
        });
    }
    Ok(())
}

/// Decode an in-memory file.
pub fn decode_bytes(
    data: Bytes,
    options: &EbcdicOptions,
    batch_size: usize,
) -> Result<(SchemaRef, BatchIter)> {
    let layout = layout_of(&options.copybook)?;
    check_length(data.len() as u64, layout.lrecl)?;
    let decoder = decoder_for(&layout, options, batch_size)?;
    let schema = decoder.schema();
    Ok((
        schema,
        Box::new(BytesBatches {
            data,
            lrecl: layout.lrecl,
            batch_size: batch_size.max(1),
            position: 0,
            decoder,
        }),
    ))
}

/// Decode a local file, one record at a time. Unlike the workbook reader this
/// needs no worker thread — nothing borrows the source.
pub fn decode_path(
    path: &Path,
    options: &EbcdicOptions,
    batch_size: usize,
) -> Result<(SchemaRef, BatchIter)> {
    let layout = layout_of(&options.copybook)?;
    check_length(std::fs::metadata(path)?.len(), layout.lrecl)?;
    let decoder = decoder_for(&layout, options, batch_size)?;
    let schema = decoder.schema();
    Ok((
        schema,
        Box::new(FileBatches {
            reader: BufReader::new(File::open(path)?),
            record: vec![0u8; layout.lrecl],
            batch_size: batch_size.max(1),
            done: false,
            decoder,
        }),
    ))
}

fn decoder_for(
    layout: &Arc<Layout>,
    options: &EbcdicOptions,
    batch_size: usize,
) -> Result<RecordDecoder> {
    RecordDecoder::new(layout.clone(), options.codepage, TRIM, batch_size.max(1))
}

struct BytesBatches {
    data: Bytes,
    lrecl: usize,
    batch_size: usize,
    position: usize,
    decoder: RecordDecoder,
}

impl Iterator for BytesBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.decoder.rows() < self.batch_size && self.position + self.lrecl <= self.data.len()
        {
            self.decoder
                .push(&self.data[self.position..self.position + self.lrecl]);
            self.position += self.lrecl;
        }
        (self.decoder.rows() > 0).then(|| self.decoder.finish())
    }
}

struct FileBatches {
    reader: BufReader<File>,
    record: Vec<u8>,
    batch_size: usize,
    done: bool,
    decoder: RecordDecoder,
}

impl Iterator for FileBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done && self.decoder.rows() < self.batch_size {
            match self.reader.read_exact(&mut self.record) {
                Ok(()) => self.decoder.push(&self.record),
                // The length check ran against the file's size, so the only
                // clean stop is a read that finds nothing left.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => self.done = true,
                Err(e) => {
                    self.done = true;
                    return Some(Err(TransporterError::from(e)));
                }
            }
        }
        (self.decoder.rows() > 0).then(|| self.decoder.finish())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use codepage::Codepage;

    // --- fixtures -------------------------------------------------------

    /// A copybook in fixed format — six sequence columns, then the code.
    fn book(lines: &[&str]) -> String {
        lines
            .iter()
            .map(|l| format!("      {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn resolved(lines: &[&str]) -> EbcdicOptions {
        with_codepage(lines, Codepage::Cp037)
    }

    fn with_codepage(lines: &[&str], codepage: Codepage) -> EbcdicOptions {
        EbcdicOptions {
            copybook: book(lines),
            codepage,
        }
    }

    /// ASCII → EBCDIC through the code page's own table.
    fn ebc(text: &str) -> Vec<u8> {
        let table = Codepage::Cp037.table();
        text.chars()
            .map(|c| table.iter().position(|&t| t == c).expect("in cp037") as u8)
            .collect()
    }

    /// A zoned field with the sign overpunched into the final byte.
    fn zoned(digits: &str, negative: bool) -> Vec<u8> {
        let mut bytes = ebc(digits);
        if let Some(last) = bytes.last_mut() {
            *last = (*last & 0x0F) | if negative { 0xD0 } else { 0xC0 };
        }
        bytes
    }

    fn packed(digits: &str, negative: bool) -> Vec<u8> {
        let padded = if digits.len().is_multiple_of(2) {
            format!("0{digits}")
        } else {
            digits.to_string()
        };
        let nibbles: Vec<u8> = padded.bytes().map(|b| b - b'0').collect();
        nibbles
            .chunks(2)
            .map(|pair| match pair {
                [hi, lo] => (hi << 4) | lo,
                [hi] => (hi << 4) | if negative { 0x0D } else { 0x0C },
                _ => 0,
            })
            .collect()
    }

    fn record(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.concat()
    }

    fn collect(
        options: EbcdicOptions,
        data: &[u8],
        batch_size: usize,
    ) -> (SchemaRef, Vec<RecordBatch>) {
        let (schema, iter) =
            decode_bytes(Bytes::copy_from_slice(data), &options, batch_size).expect("decodes");
        let batches: Vec<RecordBatch> = iter.map(|b| b.expect("batch decodes")).collect();
        (schema, batches)
    }

    fn one_batch(options: EbcdicOptions, data: &[u8]) -> RecordBatch {
        let (_, mut batches) = collect(options, data, 8192);
        assert_eq!(batches.len(), 1, "expected exactly one batch");
        batches.remove(0)
    }

    fn strings(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let index = batch.schema().index_of(column).expect("column exists");
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i).to_string()))
            .collect()
    }

    fn ints(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
        let index = batch.schema().index_of(column).expect("column exists");
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i)))
            .collect()
    }

    fn decimals(batch: &RecordBatch, column: &str) -> Vec<Option<i128>> {
        let index = batch.schema().index_of(column).expect("column exists");
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal column");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i)))
            .collect()
    }

    fn floats(batch: &RecordBatch, column: &str) -> Vec<Option<f64>> {
        let index = batch.schema().index_of(column).expect("column exists");
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 column");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i)))
            .collect()
    }

    // --- schema ----------------------------------------------------------

    #[test]
    fn the_copybook_alone_decides_the_schema() {
        let layout = resolved(&[
            "01  REC.",
            "    05  NAME    PIC X(6).",
            "    05  COUNT   PIC 9(4).",
            "    05  AMOUNT  PIC S9(7)V99 COMP-3.",
            "    05  RATE    COMP-2.",
            "    05  EDITED  PIC ZZ9.99.",
        ]);
        let schema = schema(&layout).expect("schema");
        let types: Vec<(&str, &DataType)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();
        assert_eq!(
            types,
            vec![
                ("NAME", &DataType::Utf8),
                ("COUNT", &DataType::Int64),
                ("AMOUNT", &DataType::Decimal128(9, 2)),
                ("RATE", &DataType::Float64),
                ("EDITED", &DataType::Decimal128(5, 2)),
            ]
        );
    }

    #[test]
    fn every_column_is_nullable_because_a_fixed_record_has_no_null_marker() {
        let schema = schema(&resolved(&["01  REC.", "    05  A PIC X(3)."])).unwrap();
        assert!(schema.fields().iter().all(|f| f.is_nullable()));
    }

    #[test]
    fn an_unscaled_field_is_an_int_whatever_its_storage() {
        // A three-digit COMP and a three-digit DISPLAY describe the same
        // quantity, so the schema must not turn on the storage detail.
        let layout = resolved(&[
            "01  REC.",
            "    05  A PIC S9(3).",
            "    05  B PIC S9(3) COMP.",
            "    05  C PIC S9(3) COMP-3.",
        ]);
        let schema = schema(&layout).unwrap();
        for field in schema.fields() {
            assert_eq!(field.data_type(), &DataType::Int64, "{}", field.name());
        }
    }

    #[test]
    fn a_field_wider_than_an_int_becomes_a_decimal() {
        let layout = resolved(&["01  REC.", "    05  A PIC 9(19)."]);
        assert_eq!(
            schema(&layout).unwrap().field(0).data_type(),
            &DataType::Decimal128(19, 0)
        );
    }

    #[test]
    fn a_field_wider_than_a_decimal_is_refused() {
        let layout = resolved(&["01  REC.", "    05  A PIC 9(39)."]);
        let message = schema(&layout).unwrap_err().to_string();
        assert!(message.contains("39 digits"), "{message}");
        assert!(message.contains("38"), "{message}");
    }

    #[test]
    fn a_short_float_still_decodes_wide() {
        // IBM hex float reaches 16^63, which overflows f32.
        let layout = resolved(&["01  REC.", "    05  A COMP-1."]);
        assert_eq!(
            schema(&layout).unwrap().field(0).data_type(),
            &DataType::Float64
        );
    }

    // --- decoding ----------------------------------------------------------

    #[test]
    fn a_record_decodes_field_by_field() {
        let layout = resolved(&[
            "01  REC.",
            "    05  NAME   PIC X(6).",
            "    05  COUNT  PIC 9(4).",
            "    05  AMOUNT PIC S9(5)V99 COMP-3.",
        ]);
        let data = record(&[
            record(&[ebc("ALICE"), ebc(" ")]),
            ebc("0042"),
            packed("1234567", false),
        ]);
        let batch = one_batch(layout, &data);

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(strings(&batch, "NAME"), vec![Some("ALICE".to_string())]);
        assert_eq!(ints(&batch, "COUNT"), vec![Some(42)]);
        // The decimal holds the unscaled integer; the scale lives in the type.
        assert_eq!(decimals(&batch, "AMOUNT"), vec![Some(1_234_567)]);
    }

    #[test]
    fn several_records_decode_in_order() {
        let layout = resolved(&["01  REC.", "    05  N PIC 9(3)."]);
        let data = record(&[ebc("001"), ebc("002"), ebc("003")]);
        let batch = one_batch(layout, &data);
        assert_eq!(ints(&batch, "N"), vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn signs_survive_the_round_trip() {
        let layout = resolved(&[
            "01  REC.",
            "    05  Z PIC S9(3).",
            "    05  P PIC S9(3) COMP-3.",
            "    05  B PIC S9(4) COMP.",
        ]);
        let data = record(&[
            record(&[zoned("123", true), packed("456", true), vec![0xFF, 0x85]]),
            record(&[zoned("123", false), packed("456", false), vec![0x00, 0x7B]]),
        ]);
        let batch = one_batch(layout, &data);
        assert_eq!(ints(&batch, "Z"), vec![Some(-123), Some(123)]);
        assert_eq!(ints(&batch, "P"), vec![Some(-456), Some(456)]);
        assert_eq!(ints(&batch, "B"), vec![Some(-123), Some(123)]);
    }

    #[test]
    fn a_blank_field_is_null_in_every_kind() {
        let layout = resolved(&[
            "01  REC.",
            "    05  T PIC X(4).",
            "    05  Z PIC 9(4).",
            "    05  P PIC S9(3) COMP-3.",
            "    05  E PIC ZZ9.99.",
        ]);
        let blanks = vec![0x40u8; 4 + 4 + 2 + 6];
        let batch = one_batch(layout, &blanks);
        assert_eq!(strings(&batch, "T"), vec![None]);
        assert_eq!(ints(&batch, "Z"), vec![None]);
        assert_eq!(ints(&batch, "P"), vec![None]);
        assert_eq!(decimals(&batch, "E"), vec![None]);
    }

    #[test]
    fn a_filler_gap_is_skipped_without_shifting_the_fields_after_it() {
        let layout = resolved(&[
            "01  REC.",
            "    05  A      PIC X(2).",
            "    05  FILLER PIC X(5).",
            "    05  B      PIC X(2).",
        ]);
        assert_eq!(layout_of(&layout.copybook).unwrap().lrecl, 9);
        let batch = one_batch(layout, &ebc("AAxxxxxBB"));
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(strings(&batch, "A"), vec![Some("AA".to_string())]);
        assert_eq!(strings(&batch, "B"), vec![Some("BB".to_string())]);
    }

    #[test]
    fn an_occurs_table_becomes_one_column_per_element() {
        let layout = resolved(&["01  REC.", "    05  N PIC 9(2) OCCURS 3."]);
        let batch = one_batch(layout, &ebc("010203"));
        assert_eq!(ints(&batch, "N_1"), vec![Some(1)]);
        assert_eq!(ints(&batch, "N_2"), vec![Some(2)]);
        assert_eq!(ints(&batch, "N_3"), vec![Some(3)]);
    }

    #[test]
    fn a_redefinition_and_its_target_both_read_the_same_bytes() {
        let layout = resolved(&[
            "01  REC.",
            "    05  RAW PIC X(4).",
            "    05  NUM REDEFINES RAW PIC 9(4).",
        ]);
        assert_eq!(layout_of(&layout.copybook).unwrap().lrecl, 4);
        let batch = one_batch(layout, &ebc("1234"));
        assert_eq!(strings(&batch, "RAW"), vec![Some("1234".to_string())]);
        assert_eq!(ints(&batch, "NUM"), vec![Some(1234)]);
    }

    #[test]
    fn a_comp_1_reads_as_ibm_hex_float() {
        // 0x41100000 is 1.0 as hex float and ~9.0 as IEEE, so the value
        // alone says which reading the pipeline uses.
        let layout = resolved(&["01  REC.", "    05  A COMP-1."]);
        let batch = one_batch(layout, &[0x41, 0x10, 0x00, 0x00]);
        assert_eq!(floats(&batch, "A"), vec![Some(1.0)]);
    }

    #[test]
    fn trailing_spaces_are_dropped_from_a_text_field() {
        // Padding to the field width is never part of the value.
        let layout = resolved(&["01  REC.", "    05  A PIC X(5)."]);
        let batch = one_batch(layout, &ebc("AB   "));
        assert_eq!(strings(&batch, "A"), vec![Some("AB".to_string())]);
    }

    #[test]
    fn the_code_page_reaches_the_text_fields() {
        let lines = ["01  REC.", "    05  A PIC X(1)."];
        // 0x4A is a cent sign under cp037 and A-umlaut under cp273.
        for (page, expected) in [(Codepage::Cp037, "\u{A2}"), (Codepage::Cp273, "\u{C4}")] {
            let layout = with_codepage(&lines, page);
            assert_eq!(
                strings(&one_batch(layout, &[0x4A]), "A"),
                vec![Some(expected.to_string())],
                "{page:?}"
            );
        }
    }

    // --- record framing -------------------------------------------------------

    #[test]
    fn rows_are_cut_into_batches_of_the_requested_size() {
        let layout = resolved(&["01  REC.", "    05  N PIC 9(2)."]);
        let data: Vec<u8> = (0..250).flat_map(|_| ebc("42")).collect();
        let (_, batches) = collect(layout, &data, 100);
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![100, 100, 50]
        );
    }

    #[test]
    fn an_empty_file_decodes_to_no_batches() {
        let layout = resolved(&["01  REC.", "    05  N PIC 9(2)."]);
        let (schema, batches) = collect(layout, &[], 8192);
        assert!(batches.is_empty());
        assert_eq!(schema.fields().len(), 1);
    }

    /// The single most informative error this reader produces: it catches the
    /// wrong copybook version and a file transferred in text mode.
    #[test]
    fn a_file_that_is_not_whole_records_is_refused_before_anything_is_decoded() {
        let layout = resolved(&["01  REC.", "    05  N PIC 9(4)."]);
        let data = ebc("00010002x");
        let error = decode_bytes(Bytes::from(data), &layout, 8192)
            .err()
            .expect("a 9-byte file is not whole 4-byte records");
        let message = error.to_string();
        assert!(message.contains("9 bytes"), "{message}");
        assert!(message.contains("4-byte records"), "{message}");
        assert!(message.contains("1 left over"), "{message}");
        assert!(message.contains("text mode"), "{message}");
    }

    #[test]
    fn streaming_a_file_gives_exactly_what_the_in_memory_path_gives() {
        let layout = resolved(&[
            "01  REC.",
            "    05  NAME PIC X(4).",
            "    05  N    PIC S9(3) COMP-3.",
        ]);
        let data = record(&[
            record(&[ebc("ABCD"), packed("123", false)]),
            record(&[ebc("EFGH"), packed("456", true)]),
        ]);

        let dir =
            std::env::temp_dir().join(format!("td-ebcdic-test-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("records.dat");
        std::fs::write(&path, &data).unwrap();

        let (_, from_memory) = collect(layout.clone(), &data, 8192);
        let (_, iter) = decode_path(&path, &layout, 8192).expect("decodes");
        let from_file: Vec<RecordBatch> = iter.map(|b| b.unwrap()).collect();

        assert_eq!(from_memory, from_file);
        assert_eq!(ints(&from_file[0], "N"), vec![Some(123), Some(-456)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_streamed_file_is_also_length_checked() {
        let layout = resolved(&["01  REC.", "    05  N PIC 9(4)."]);
        let dir =
            std::env::temp_dir().join(format!("td-ebcdic-test-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ragged.dat");
        std::fs::write(&path, ebc("00010002x")).unwrap();

        let error = decode_path(&path, &layout, 8192).err().expect("refused");
        assert!(error.to_string().contains("not a whole number"), "{error}");
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- parsing the inline copybook ----------------------------------------

    #[test]
    fn the_inline_copybook_is_the_whole_schema_source() {
        // No uri, no fetch: the layout arrives with the request.
        let options = resolved(&["01  REC.", "    05  A PIC X(4).", "    05  B PIC 9(3)."]);
        let layout = layout_of(&options.copybook).expect("parses");
        assert_eq!(layout.lrecl, 7);
        assert_eq!(layout.cols.len(), 2);
    }

    #[test]
    fn the_schema_reads_no_data_at_all() {
        let options = resolved(&["01  REC.", "    05  A PIC X(4)."]);
        let schema = schema(&options).expect("schema");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "A");
    }

    #[test]
    fn a_copybook_that_does_not_parse_fails_before_any_decoding() {
        let options = resolved(&["01  REC.", "    05  A PIC X(4) WOBBLE."]);
        let message = layout_of(&options.copybook).unwrap_err().to_string();
        assert!(message.contains("unrecognized clause"), "{message}");
        // And it reaches a caller that went straight to decoding.
        assert!(decode_bytes(Bytes::from_static(b"1234"), &options, 8192).is_err());
    }

    #[test]
    fn an_empty_copybook_is_refused() {
        let options = EbcdicOptions {
            copybook: String::new(),
            codepage: Codepage::Cp037,
        };
        let message = layout_of(&options.copybook).unwrap_err().to_string();
        assert!(message.contains("no data description entries"), "{message}");
    }

    #[test]
    fn the_same_copybook_text_is_parsed_once() {
        let text = book(&["01  CACHED.", "    05  A PIC X(7)."]);
        let first = layout_of(&text).expect("parses");
        let second = layout_of(&text).expect("parses");
        assert!(
            Arc::ptr_eq(&first, &second),
            "paths sharing a layout must parse it once"
        );
    }

    #[test]
    fn two_different_copybooks_do_not_share_a_layout() {
        let one = layout_of(&book(&["01  A.", "    05  X PIC X(3)."])).unwrap();
        let two = layout_of(&book(&["01  B.", "    05  X PIC X(9)."])).unwrap();
        assert_eq!((one.lrecl, two.lrecl), (3, 9));
    }

    #[test]
    fn the_code_page_is_the_only_other_thing_on_the_wire() {
        // Everything else is fixed and tested above; this pins that the
        // options struct cannot vary it.
        let options = resolved(&["01  REC.", "    05  A PIC X(1)."]);
        assert_eq!(
            options,
            EbcdicOptions {
                copybook: options.copybook.clone(),
                codepage: Codepage::Cp037,
            }
        );
    }
}
