//
// Copyright 2026 Tabsdata Inc.
//

//! CSV reader-side option support. [`ResolvedCsv`] validates the wire
//! [`CsvOptions`] once and then serves two roles: it builds the arrow
//! [`Format`](arrow::csv::reader::Format) carrying the natively-supported
//! knobs (header, delimiter, quote, comment, null values, short rows), and it
//! wraps the raw input in streaming [`Read`] adapters for what arrow cannot
//! express — `skip_rows` (raw line skip), `skip_rows_after_header`,
//! `truncate_ragged_lines` (over-long rows) and `decimal_comma`. The adapters
//! process the stream incrementally, so memory stays bounded by record size,
//! never file size.

use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::spec::CsvOptions;
use std::io::Read;

#[derive(Debug, Clone)]
pub struct ResolvedCsv {
    has_header: bool,
    delimiter: u8,
    quote: u8,
    comment: Option<u8>,
    null_regex: Option<regex::Regex>,
    decimal_comma: bool,
    truncate_ragged_lines: bool,
    skip_rows: usize,
    skip_rows_after_header: usize,
}

fn ascii_byte(name: &str, value: char) -> Result<u8> {
    if value.is_ascii() {
        Ok(value as u8)
    } else {
        Err(TransporterError::InvalidCsvOptions(format!(
            "{name} must be a single ASCII character, got '{value}'"
        )))
    }
}

impl ResolvedCsv {
    pub fn try_new(options: &CsvOptions) -> Result<Self> {
        let delimiter = ascii_byte("delimiter", options.delimiter)?;
        let quote = ascii_byte("quote_char", options.quote_char)?;
        let comment = match options.comment_prefix.as_deref() {
            None | Some("") => None,
            Some(prefix) => {
                let mut chars = prefix.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) => Some(ascii_byte("comment_prefix", c)?),
                    _ => {
                        return Err(TransporterError::InvalidCsvOptions(format!(
                            "comment_prefix must be a single ASCII character, got '{prefix}'"
                        )));
                    }
                }
            }
        };
        if options.decimal_comma && delimiter == b',' {
            return Err(TransporterError::InvalidCsvOptions(
                "decimal_comma requires a delimiter other than ','".to_string(),
            ));
        }
        let null_regex = match options.null_values.as_deref() {
            None | Some([]) => None,
            Some(values) => {
                let alternation = values
                    .iter()
                    .map(|v| regex::escape(v))
                    .collect::<Vec<_>>()
                    .join("|");
                Some(
                    regex::Regex::new(&format!("^(?:{alternation})$")).map_err(|e| {
                        TransporterError::InvalidCsvOptions(format!("null_values: {e}"))
                    })?,
                )
            }
        };
        Ok(Self {
            has_header: options.has_header,
            delimiter,
            quote,
            comment,
            null_regex,
            decimal_comma: options.decimal_comma,
            truncate_ragged_lines: options.truncate_ragged_lines,
            skip_rows: options.skip_rows,
            skip_rows_after_header: options.skip_rows_after_header,
        })
    }

    /// The arrow-side format, carrying every natively-supported option. Used
    /// for both schema inference and batch decoding (over a [`wrap`]ped
    /// input, so both see the same preprocessed stream).
    ///
    /// [`wrap`]: Self::wrap
    pub fn arrow_format(&self) -> arrow::csv::reader::Format {
        let mut format = arrow::csv::reader::Format::default()
            .with_header(self.has_header)
            .with_delimiter(self.delimiter)
            .with_quote(self.quote)
            .with_truncated_rows(self.truncate_ragged_lines);
        if let Some(comment) = self.comment {
            format = format.with_comment(comment);
        }
        if let Some(null_regex) = &self.null_regex {
            format = format.with_null_regex(null_regex.clone());
        }
        format
    }

    /// Chain the preprocessing adapters over `input`. A no-op (beyond the
    /// box) when no non-native option is set.
    pub fn wrap<R: Read + Send + 'static>(&self, input: R) -> Box<dyn Read + Send> {
        let mut stream: Box<dyn Read + Send> = Box::new(input);
        if self.skip_rows > 0 {
            stream = Box::new(SkipLines::new(stream, self.skip_rows));
        }
        if self.needs_record_pass() {
            stream = Box::new(RecordPass::new(stream, self));
        }
        stream
    }

    fn needs_record_pass(&self) -> bool {
        self.truncate_ragged_lines || self.decimal_comma || self.skip_rows_after_header > 0
    }
}

/// Drops the first `remaining` raw lines of the inner stream, then passes the
/// rest through untouched. Raw (quote-unaware) on purpose: `skip_rows` skips
/// physical lines before CSV parsing starts.
struct SkipLines<R> {
    inner: R,
    remaining: usize,
    carry: Vec<u8>,
    carry_pos: usize,
}

impl<R: Read> SkipLines<R> {
    fn new(inner: R, remaining: usize) -> Self {
        Self {
            inner,
            remaining,
            carry: Vec::new(),
            carry_pos: 0,
        }
    }
}

impl<R: Read> Read for SkipLines<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let mut chunk = [0u8; 8192];
        while self.remaining > 0 {
            let n = self.inner.read(&mut chunk)?;
            if n == 0 {
                self.remaining = 0;
                break;
            }
            let mut start = 0;
            while self.remaining > 0 {
                match chunk[start..n].iter().position(|&b| b == b'\n') {
                    Some(offset) => {
                        start += offset + 1;
                        self.remaining -= 1;
                    }
                    None => break,
                }
            }
            if self.remaining == 0 && start < n {
                self.carry.extend_from_slice(&chunk[start..n]);
            }
        }
        if self.carry_pos < self.carry.len() {
            let len = out.len().min(self.carry.len() - self.carry_pos);
            out[..len].copy_from_slice(&self.carry[self.carry_pos..self.carry_pos + len]);
            self.carry_pos += len;
            if self.carry_pos == self.carry.len() {
                self.carry.clear();
                self.carry_pos = 0;
            }
            return Ok(len);
        }
        self.inner.read(out)
    }
}

/// Record-level pass: parses the stream with a tolerant CSV reader and
/// re-serializes it, dropping the `skip_rows_after_header` records, truncating
/// over-long rows to the width of the first record, and rewriting
/// whole-field decimal commas (`1,5` → `1.5`). Pulls records lazily as the
/// consumer reads, one output chunk at a time.
struct RecordPass<R: Read> {
    records: csv::ByteRecordsIntoIter<R>,
    delimiter: u8,
    quote: u8,
    decimal_comma: bool,
    truncate: bool,
    width: Option<usize>,
    header_pending: bool,
    skip_pending: usize,
    buf: Vec<u8>,
    pos: usize,
    done: bool,
}

const RECORD_PASS_CHUNK: usize = 64 * 1024;

impl<R: Read> RecordPass<R> {
    fn new(inner: R, resolved: &ResolvedCsv) -> Self {
        let mut builder = csv::ReaderBuilder::new();
        builder
            .has_headers(false)
            .flexible(true)
            .delimiter(resolved.delimiter)
            .quote(resolved.quote)
            .comment(resolved.comment);
        Self {
            records: builder.from_reader(inner).into_byte_records(),
            delimiter: resolved.delimiter,
            quote: resolved.quote,
            decimal_comma: resolved.decimal_comma,
            truncate: resolved.truncate_ragged_lines,
            width: None,
            header_pending: resolved.has_header,
            skip_pending: resolved.skip_rows_after_header,
            buf: Vec::new(),
            pos: 0,
            done: false,
        }
    }

    fn fill(&mut self) -> std::io::Result<()> {
        self.buf.clear();
        self.pos = 0;
        let mut writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .flexible(true)
            .from_writer(&mut self.buf);
        while writer.get_ref().len() < RECORD_PASS_CHUNK {
            let Some(record) = self.records.next() else {
                self.done = true;
                break;
            };
            let record = record.map_err(std::io::Error::other)?;
            if self.header_pending {
                self.header_pending = false;
                self.width = Some(record.len());
                writer.write_byte_record(&record)?;
                continue;
            }
            if self.skip_pending > 0 {
                self.skip_pending -= 1;
                continue;
            }
            let width = *self.width.get_or_insert(record.len());
            let mut out = csv::ByteRecord::new();
            for field in record
                .iter()
                .take(if self.truncate { width } else { usize::MAX })
            {
                match self
                    .decimal_comma
                    .then(|| decimal_comma_to_dot(field))
                    .flatten()
                {
                    Some(rewritten) => out.push_field(&rewritten),
                    None => out.push_field(field),
                }
            }
            writer.write_byte_record(&out)?;
        }
        writer.flush()?;
        Ok(())
    }
}

impl<R: Read> Read for RecordPass<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.buf.len() {
            if self.done {
                return Ok(0);
            }
            self.fill()?;
        }
        let len = out.len().min(self.buf.len() - self.pos);
        out[..len].copy_from_slice(&self.buf[self.pos..self.pos + len]);
        self.pos += len;
        Ok(len)
    }
}

/// If `field` is exactly a decimal-comma number (`-1,5`, `2,75e3`), return it
/// with the comma replaced by a dot; otherwise `None`. Whole-field matching
/// keeps commas inside genuine text values untouched.
fn decimal_comma_to_dot(field: &[u8]) -> Option<Vec<u8>> {
    let mut i = 0;
    if matches!(field.first(), Some(b'+') | Some(b'-')) {
        i = 1;
    }
    let int_start = i;
    while i < field.len() && field[i].is_ascii_digit() {
        i += 1;
    }
    if i == int_start || i >= field.len() || field[i] != b',' {
        return None;
    }
    let comma = i;
    i += 1;
    let frac_start = i;
    while i < field.len() && field[i].is_ascii_digit() {
        i += 1;
    }
    if i == frac_start {
        return None;
    }
    if i < field.len() {
        if !matches!(field[i], b'e' | b'E') {
            return None;
        }
        i += 1;
        if matches!(field.get(i), Some(b'+') | Some(b'-')) {
            i += 1;
        }
        let exp_start = i;
        while i < field.len() && field[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start || i < field.len() {
            return None;
        }
    }
    let mut out = field.to_vec();
    out[comma] = b'.';
    Some(out)
}

#[cfg(test)]
mod test {
    use super::*;

    fn read_all(mut r: impl Read) -> String {
        let mut s = String::new();
        r.read_to_string(&mut s).unwrap();
        s
    }

    fn resolved(options: CsvOptions) -> ResolvedCsv {
        ResolvedCsv::try_new(&options).unwrap()
    }

    #[test]
    fn skip_lines_drops_leading_lines_only() {
        let input = "noise 1\nnoise 2\nid,name\n1,alice\n";
        let out = read_all(SkipLines::new(input.as_bytes(), 2));
        assert_eq!(out, "id,name\n1,alice\n");
    }

    #[test]
    fn skip_lines_past_eof_yields_empty() {
        let out = read_all(SkipLines::new("only line\n".as_bytes(), 5));
        assert_eq!(out, "");
    }

    #[test]
    fn record_pass_skips_rows_after_header() {
        let csv = resolved(CsvOptions {
            skip_rows_after_header: 2,
            ..Default::default()
        });
        let input = "id,name\nskip_1,skip_1\nskip_2,skip_2\n1,alice\n";
        let out = read_all(csv.wrap(input.as_bytes()));
        assert_eq!(out, "id,name\n1,alice\n");
    }

    #[test]
    fn record_pass_truncates_ragged_rows() {
        let csv = resolved(CsvOptions {
            truncate_ragged_lines: true,
            ..Default::default()
        });
        let input = "id,name\n1,alice\n2,bob,extra\n";
        let out = read_all(csv.wrap(input.as_bytes()));
        assert_eq!(out, "id,name\n1,alice\n2,bob\n");
    }

    #[test]
    fn record_pass_rewrites_whole_field_decimal_commas() {
        let csv = resolved(CsvOptions {
            delimiter: ';',
            decimal_comma: true,
            ..Default::default()
        });
        let input = "id;value;note\n1;1,5;a 1,2 b\n2;2,75e2;plain\n";
        let out = read_all(csv.wrap(input.as_bytes()));
        assert_eq!(out, "id;value;note\n1;1.5;a 1,2 b\n2;2.75e2;plain\n");
    }

    #[test]
    fn decimal_comma_with_comma_delimiter_is_rejected() {
        let err = ResolvedCsv::try_new(&CsvOptions {
            decimal_comma: true,
            ..Default::default()
        })
        .unwrap_err();
        assert!(matches!(err, TransporterError::InvalidCsvOptions(_)));
    }

    #[test]
    fn multichar_comment_prefix_is_rejected() {
        let err = ResolvedCsv::try_new(&CsvOptions {
            comment_prefix: Some("//".to_string()),
            ..Default::default()
        })
        .unwrap_err();
        assert!(matches!(err, TransporterError::InvalidCsvOptions(_)));
    }

    #[test]
    fn decimal_comma_to_dot_matches_only_whole_numbers() {
        assert_eq!(decimal_comma_to_dot(b"1,5"), Some(b"1.5".to_vec()));
        assert_eq!(decimal_comma_to_dot(b"-12,75"), Some(b"-12.75".to_vec()));
        assert_eq!(decimal_comma_to_dot(b"1,5e-3"), Some(b"1.5e-3".to_vec()));
        assert_eq!(decimal_comma_to_dot(b"a 1,2 b"), None);
        assert_eq!(decimal_comma_to_dot(b"1,5,7"), None);
        assert_eq!(decimal_comma_to_dot(b"1,"), None);
        assert_eq!(decimal_comma_to_dot(b",5"), None);
        assert_eq!(decimal_comma_to_dot(b"15"), None);
    }
}
