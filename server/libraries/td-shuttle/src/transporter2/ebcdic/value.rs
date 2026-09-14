//
// Copyright 2026 Tabsdata Inc.
//

//! Field decoders: one field's raw bytes → a typed value, or `None` for null.
//!
//! Zoned and packed decimals carry their sign in a nibble of a digit byte, so
//! translating them to characters would destroy them — only [`text`] and
//! [`edited`] take a [`Codepage`]. It is also why zoned decoding is code-page
//! independent: every EBCDIC and ASCII encoding puts 0-9 in the low nibble.
//!
//! An all-spaces or all-low-values field is null: fixed records have no null
//! marker and an uninitialised field is routine.

use crate::transporter2::ebcdic::codepage::Codepage;
use crate::transporter2::ebcdic::copybook::SignPos;

/// The three ways a fixed record spells "nothing here".
fn is_blank(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0x40 || b == 0x20 || b == 0x00)
}

/// `Some(true)` if negative; `None` if the byte is not a sign at all.
fn separate_sign(b: u8) -> Option<bool> {
    match b {
        0x60 | 0x2D => Some(true),                // EBCDIC / ASCII '-'
        0x4E | 0x2B | 0x40 | 0x20 => Some(false), // '+' or blank
        _ => None,
    }
}

/// `D` and `B` are negative; `C`, `A`, `E` and `F` positive.
fn overpunch_negative(nibble: u8) -> bool {
    matches!(nibble, 0x0D | 0x0B)
}

/// Zoned decimal (`USAGE DISPLAY`) → the unscaled integer.
pub fn zoned(bytes: &[u8], signed: bool, sign: SignPos) -> Option<i128> {
    if bytes.is_empty() || is_blank(bytes) {
        return None;
    }
    let (digits, mut negative) = match sign {
        SignPos::LeadingSeparate => {
            let (first, rest) = bytes.split_first()?;
            (rest, separate_sign(*first)?)
        }
        SignPos::TrailingSeparate => {
            let (last, rest) = bytes.split_last()?;
            (rest, separate_sign(*last)?)
        }
        SignPos::Overpunch => (bytes, false),
    };
    if digits.is_empty() {
        return None;
    }

    let mut value: i128 = 0;
    let last_index = digits.len() - 1;
    for (i, &b) in digits.iter().enumerate() {
        let digit = i128::from(b & 0x0F);
        if digit > 9 {
            return None;
        }
        // Trailing overpunch is the COBOL default for a signed DISPLAY item.
        if i == last_index && signed && sign == SignPos::Overpunch && overpunch_negative(b >> 4) {
            negative = true;
        }
        value = value.checked_mul(10)?.checked_add(digit)?;
    }
    Some(if negative { -value } else { value })
}

/// Packed decimal (`COMP-3`) → the unscaled integer. Two digits per byte, the
/// final low nibble the sign — always present, so there is no unsigned COMP-3.
pub fn packed(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || is_blank(bytes) {
        return None;
    }
    let mut value: i128 = 0;
    let last_index = bytes.len() - 1;
    for (i, &b) in bytes.iter().enumerate() {
        let high = i128::from(b >> 4);
        if high > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(high)?;
        if i != last_index {
            let low = i128::from(b & 0x0F);
            if low > 9 {
                return None;
            }
            value = value.checked_mul(10)?.checked_add(low)?;
        }
    }
    let negative = overpunch_negative(bytes[last_index] & 0x0F);
    Some(if negative { -value } else { value })
}

/// Big-endian two's-complement binary (`COMP`) → the unscaled integer.
pub fn binary(bytes: &[u8], signed: bool) -> Option<i128> {
    match (bytes.len(), signed) {
        (2, true) => Some(i128::from(i16::from_be_bytes([bytes[0], bytes[1]]))),
        (2, false) => Some(i128::from(u16::from_be_bytes([bytes[0], bytes[1]]))),
        (4, true) => Some(i128::from(i32::from_be_bytes(bytes.try_into().ok()?))),
        (4, false) => Some(i128::from(u32::from_be_bytes(bytes.try_into().ok()?))),
        (8, true) => Some(i128::from(i64::from_be_bytes(bytes.try_into().ok()?))),
        (8, false) => Some(i128::from(u64::from_be_bytes(bytes.try_into().ok()?))),
        _ => None,
    }
}

/// IBM hex float, short: sign, 7-bit excess-64 base-16 exponent, 24-bit fraction.
pub fn hfp32(bytes: [u8; 4]) -> f64 {
    let raw = u32::from_be_bytes(bytes);
    let fraction = f64::from(raw & 0x00FF_FFFF) / f64::from(1u32 << 24);
    hfp(raw >> 31 == 1, ((raw >> 24) & 0x7F) as i32, fraction)
}

/// IBM hex float, long: 56-bit fraction.
pub fn hfp64(bytes: [u8; 8]) -> f64 {
    let raw = u64::from_be_bytes(bytes);
    let fraction = (raw & 0x00FF_FFFF_FFFF_FFFF) as f64 / (1u64 << 56) as f64;
    hfp(raw >> 63 == 1, ((raw >> 56) & 0x7F) as i32, fraction)
}

fn hfp(negative: bool, biased_exponent: i32, fraction: f64) -> f64 {
    let value = fraction * 16f64.powi(biased_exponent - 64);
    if negative { -value } else { value }
}

pub fn ieee32(bytes: [u8; 4]) -> f64 {
    f64::from(f32::from_be_bytes(bytes))
}

pub fn ieee64(bytes: [u8; 8]) -> f64 {
    f64::from_be_bytes(bytes)
}

/// A `PIC X`/`PIC A` field, translated. `trim` drops the padding a fixed-width
/// field uses to spell a shorter string.
pub fn text(bytes: &[u8], codepage: Codepage, trim: bool) -> Option<String> {
    if is_blank(bytes) {
        return None;
    }
    let decoded = codepage.decode(bytes);
    Some(if trim {
        decoded.trim_end().to_string()
    } else {
        decoded
    })
}

/// Numeric-edited: translate, strip the edit symbols. `CR`, `DB` or a `-`
/// anywhere means negative.
pub fn edited(bytes: &[u8], codepage: Codepage) -> Option<i128> {
    if is_blank(bytes) {
        return None;
    }
    let rendered = codepage.decode(bytes).to_uppercase();
    let digits: String = rendered.chars().filter(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return None;
    }
    let value: i128 = digits.parse().ok()?;
    let negative = rendered.contains("CR") || rendered.contains("DB") || rendered.contains('-');
    Some(if negative { -value } else { value })
}

#[cfg(test)]
mod test {
    use super::*;

    /// ASCII → EBCDIC, by inverting the code page's own table. Building the
    /// fixtures from the table under test would be circular, so the tests that
    /// care about the *translation* spell bytes out by hand; this is only for
    /// composing records.
    fn ebc(text: &str) -> Vec<u8> {
        let table = Codepage::Cp037.table();
        text.chars()
            .map(|c| {
                table
                    .iter()
                    .position(|&t| t == c)
                    .unwrap_or_else(|| panic!("{c:?} is not in cp037")) as u8
            })
            .collect()
    }

    /// A zoned-decimal field: EBCDIC digits, with the sign overpunched into the
    /// high nibble of the last byte when `negative`.
    fn zoned_bytes(digits: &str, negative: bool) -> Vec<u8> {
        let mut bytes = ebc(digits);
        if let Some(last) = bytes.last_mut() {
            *last = (*last & 0x0F) | if negative { 0xD0 } else { 0xC0 };
        }
        bytes
    }

    fn packed_bytes(digits: &str, negative: bool) -> Vec<u8> {
        // Pad to an odd nibble count so the sign nibble lands on a byte edge.
        let padded = if digits.len().is_multiple_of(2) {
            format!("0{digits}")
        } else {
            digits.to_string()
        };
        let nibbles: Vec<u8> = padded.bytes().map(|b| b - b'0').collect();
        let mut out = Vec::with_capacity(nibbles.len() / 2 + 1);
        for pair in nibbles.chunks(2) {
            out.push(match pair {
                [hi, lo] => (hi << 4) | lo,
                [hi] => (hi << 4) | if negative { 0x0D } else { 0x0C },
                _ => unreachable!(),
            });
        }
        out
    }

    const EBCDIC_SPACE: u8 = 0x40;

    // --- zoned --------------------------------------------------------------

    #[test]
    fn an_unsigned_zoned_field_reads_its_digits() {
        assert_eq!(
            zoned(&ebc("12345"), false, SignPos::Overpunch),
            Some(12_345)
        );
    }

    #[test]
    fn an_overpunched_sign_is_read_from_the_last_byte() {
        assert_eq!(
            zoned(&zoned_bytes("12345", false), true, SignPos::Overpunch),
            Some(12_345)
        );
        assert_eq!(
            zoned(&zoned_bytes("12345", true), true, SignPos::Overpunch),
            Some(-12_345)
        );
    }

    #[test]
    fn an_overpunched_sign_is_ignored_on_an_unsigned_field() {
        // A `PIC 9(5)` field never carries a sign, so a D nibble is just a zone.
        assert_eq!(
            zoned(&zoned_bytes("12345", true), false, SignPos::Overpunch),
            Some(12_345)
        );
    }

    #[test]
    fn every_positive_overpunch_nibble_is_positive() {
        for high in [0xC0u8, 0xA0, 0xE0, 0xF0] {
            let mut bytes = ebc("123");
            *bytes.last_mut().unwrap() = (bytes[2] & 0x0F) | high;
            assert_eq!(
                zoned(&bytes, true, SignPos::Overpunch),
                Some(123),
                "high nibble {high:#x}"
            );
        }
        for high in [0xD0u8, 0xB0] {
            let mut bytes = ebc("123");
            *bytes.last_mut().unwrap() = (bytes[2] & 0x0F) | high;
            assert_eq!(
                zoned(&bytes, true, SignPos::Overpunch),
                Some(-123),
                "high nibble {high:#x}"
            );
        }
    }

    #[test]
    fn a_separate_sign_is_read_from_its_own_byte() {
        let mut leading = vec![0x60]; // EBCDIC '-'
        leading.extend(ebc("123"));
        assert_eq!(zoned(&leading, true, SignPos::LeadingSeparate), Some(-123));

        let mut trailing = ebc("123");
        trailing.push(0x4E); // EBCDIC '+'
        assert_eq!(zoned(&trailing, true, SignPos::TrailingSeparate), Some(123));
    }

    #[test]
    fn a_blank_separate_sign_reads_as_positive() {
        let mut bytes = vec![EBCDIC_SPACE];
        bytes.extend(ebc("123"));
        assert_eq!(zoned(&bytes, true, SignPos::LeadingSeparate), Some(123));
    }

    #[test]
    fn zoned_decoding_is_code_page_independent() {
        // Every EBCDIC and ASCII encoding puts the digit in the low nibble, so
        // ASCII digits decode identically without any translation.
        assert_eq!(zoned(b"12345", false, SignPos::Overpunch), Some(12_345));
    }

    #[test]
    fn a_blank_or_low_value_zoned_field_is_null() {
        assert_eq!(zoned(&[EBCDIC_SPACE; 5], false, SignPos::Overpunch), None);
        assert_eq!(zoned(&[0x00; 5], false, SignPos::Overpunch), None);
        assert_eq!(zoned(&[b' '; 5], false, SignPos::Overpunch), None);
        assert_eq!(zoned(&[], false, SignPos::Overpunch), None);
    }

    #[test]
    fn a_non_digit_zoned_field_is_null_rather_than_a_wrong_number() {
        let mut bytes = ebc("123");
        bytes[1] = 0xFF; // low nibble F is not a digit
        assert_eq!(zoned(&bytes, false, SignPos::Overpunch), None);
    }

    #[test]
    fn a_separate_sign_byte_that_is_not_a_sign_is_null() {
        let mut bytes = ebc("X");
        bytes.extend(ebc("123"));
        assert_eq!(zoned(&bytes, true, SignPos::LeadingSeparate), None);
    }

    // --- packed --------------------------------------------------------------

    #[test]
    fn packed_reads_two_digits_a_byte_with_a_trailing_sign_nibble() {
        assert_eq!(packed(&packed_bytes("12345", false)), Some(12_345));
        assert_eq!(packed(&packed_bytes("12345", true)), Some(-12_345));
    }

    #[test]
    fn packed_handles_an_even_digit_count() {
        // 1234 pads to 01234, giving three bytes: 01 23 4C.
        let bytes = packed_bytes("1234", false);
        assert_eq!(bytes, vec![0x01, 0x23, 0x4C]);
        assert_eq!(packed(&bytes), Some(1_234));
    }

    #[test]
    fn packed_reads_a_hand_written_field() {
        // The canonical shape: +123 in three digits is 0x12 0x3C.
        assert_eq!(packed(&[0x12, 0x3C]), Some(123));
        assert_eq!(packed(&[0x12, 0x3D]), Some(-123));
        // F is an unsigned sign nibble, which reads as positive.
        assert_eq!(packed(&[0x12, 0x3F]), Some(123));
    }

    #[test]
    fn a_blank_or_low_value_packed_field_is_null() {
        assert_eq!(packed(&[0x40; 3]), None);
        assert_eq!(packed(&[0x00; 3]), None);
        assert_eq!(packed(&[]), None);
    }

    #[test]
    fn a_non_digit_packed_nibble_is_null() {
        assert_eq!(packed(&[0xA2, 0x3C]), None);
        assert_eq!(packed(&[0x1F, 0x3C]), None);
    }

    #[test]
    fn packed_zero_is_zero_not_null() {
        assert_eq!(packed(&[0x00, 0x0C]), Some(0));
    }

    // --- binary --------------------------------------------------------------

    #[test]
    fn binary_reads_big_endian_twos_complement() {
        assert_eq!(binary(&[0x00, 0x7B], true), Some(123));
        assert_eq!(binary(&[0xFF, 0x85], true), Some(-123));
        assert_eq!(binary(&[0x00, 0x00, 0x00, 0x7B], true), Some(123));
        assert_eq!(binary(&[0xFF, 0xFF, 0xFF, 0x85], true), Some(-123));
        assert_eq!(binary(&[0, 0, 0, 0, 0, 0, 0, 0x7B], true), Some(123));
    }

    #[test]
    fn an_unsigned_binary_field_does_not_go_negative() {
        assert_eq!(binary(&[0xFF, 0x85], false), Some(65_413));
        assert_eq!(binary(&[0xFF, 0x85], true), Some(-123));
    }

    #[test]
    fn binary_bounds_round_trip() {
        assert_eq!(binary(&i16::MIN.to_be_bytes(), true), Some(-32_768));
        assert_eq!(
            binary(&i64::MAX.to_be_bytes(), true),
            Some(i128::from(i64::MAX))
        );
        assert_eq!(
            binary(&u64::MAX.to_be_bytes(), false),
            Some(i128::from(u64::MAX))
        );
    }

    #[test]
    fn a_binary_field_of_an_impossible_width_is_null() {
        assert_eq!(binary(&[0x01, 0x02, 0x03], true), None);
        assert_eq!(binary(&[], true), None);
    }

    // --- floats ---------------------------------------------------------------

    #[test]
    fn hex_float_reads_the_ibm_short_form() {
        // Exponent 0x41 is 16^1; fraction 0x100000/0x1000000 is 1/16.
        assert_eq!(hfp32([0x41, 0x10, 0x00, 0x00]), 1.0);
        assert_eq!(hfp32([0xC1, 0x10, 0x00, 0x00]), -1.0);
        assert_eq!(hfp32([0x00, 0x00, 0x00, 0x00]), 0.0);
        assert_eq!(hfp32([0x40, 0x80, 0x00, 0x00]), 0.5);
        assert_eq!(hfp32([0x42, 0x64, 0x00, 0x00]), 100.0);
    }

    #[test]
    fn hex_float_reads_the_ibm_long_form() {
        assert_eq!(hfp64([0x41, 0x10, 0, 0, 0, 0, 0, 0]), 1.0);
        assert_eq!(hfp64([0xC1, 0x10, 0, 0, 0, 0, 0, 0]), -1.0);
        assert_eq!(hfp64([0x42, 0x64, 0, 0, 0, 0, 0, 0]), 100.0);
    }

    #[test]
    fn hex_float_reaches_beyond_the_range_of_an_f32() {
        // 16^63 is about 10^75, which is why COMP-1 decodes to f64.
        let big = hfp32([0x7F, 0x10, 0x00, 0x00]);
        assert!(big > f64::from(f32::MAX), "{big:e}");
    }

    #[test]
    fn ieee_floats_are_read_big_endian() {
        assert_eq!(ieee32(1.0f32.to_be_bytes()), 1.0);
        assert_eq!(ieee64((-2.5f64).to_be_bytes()), -2.5);
    }

    #[test]
    fn the_same_bytes_mean_different_numbers_under_hfp_and_ieee() {
        // Why `hfp` must be set deliberately: both readings look plausible.
        let bytes = [0x41, 0x10, 0x00, 0x00];
        assert_eq!(hfp32(bytes), 1.0);
        assert!((ieee32(bytes) - 9.0).abs() < 0.001, "{}", ieee32(bytes));
    }

    // --- text ------------------------------------------------------------------

    #[test]
    fn text_is_translated_and_trailing_spaces_are_dropped() {
        let mut bytes = ebc("ALICE");
        bytes.extend([EBCDIC_SPACE; 5]);
        assert_eq!(
            text(&bytes, Codepage::Cp037, true).as_deref(),
            Some("ALICE")
        );
        assert_eq!(
            text(&bytes, Codepage::Cp037, false).as_deref(),
            Some("ALICE     ")
        );
    }

    #[test]
    fn leading_spaces_survive_trimming() {
        let bytes = ebc("  X ");
        assert_eq!(text(&bytes, Codepage::Cp037, true).as_deref(), Some("  X"));
    }

    #[test]
    fn an_all_blank_text_field_is_null() {
        assert_eq!(text(&[EBCDIC_SPACE; 4], Codepage::Cp037, true), None);
        assert_eq!(text(&[0x00; 4], Codepage::Cp037, true), None);
    }

    // --- numeric-edited ----------------------------------------------------------

    #[test]
    fn edited_strips_the_edit_symbols() {
        assert_eq!(edited(&ebc("12,345.67"), Codepage::Cp037), Some(1_234_567));
        assert_eq!(edited(&ebc("   1,234"), Codepage::Cp037), Some(1_234));
        assert_eq!(edited(&ebc("$1,234.50"), Codepage::Cp037), Some(123_450));
    }

    #[test]
    fn edited_reads_every_spelling_of_a_negative() {
        for rendered in ["1,234.56CR", "1,234.56DB", "-1,234.56"] {
            assert_eq!(
                edited(&ebc(rendered), Codepage::Cp037),
                Some(-123_456),
                "{rendered}"
            );
        }
    }

    #[test]
    fn asterisk_protection_leaves_the_digits_readable() {
        assert_eq!(edited(&ebc("***12.34"), Codepage::Cp037), Some(1_234));
    }

    #[test]
    fn an_edited_field_with_no_digits_is_null() {
        assert_eq!(edited(&ebc("****"), Codepage::Cp037), None);
        assert_eq!(edited(&[EBCDIC_SPACE; 6], Codepage::Cp037), None);
    }
}
