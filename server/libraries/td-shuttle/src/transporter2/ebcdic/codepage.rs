//
// Copyright 2026 Tabsdata Inc.
//

//! EBCDIC → Unicode for a record's text fields. Only `Kind::Text` and
//! `Kind::Edited` come through here; the decimal encodings are nibble-based
//! and decode from raw bytes in [`super::value`].
//!
//! The tables are complete single-byte maps, so decoding cannot fail. DBCS
//! EBCDIC is refused at parse time.
//!
//! Each was generated from the CPython codec of the same name, and the tests
//! below pin the bytes where the pages diverge:
//!
//! ```text
//! python -c "[print(f'{ord(bytes([i]).decode(\"cp037\")):04X}') for i in range(256)]"
//! ```

use serde::{Deserialize, Serialize};

/// A supported single-byte EBCDIC code page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Codepage {
    /// US/Canada, and the overwhelmingly common default.
    #[default]
    Cp037,
    /// Germany / Austria.
    Cp273,
    /// International.
    Cp500,
    /// IBM-037 with the euro sign at 0x9F.
    Cp1140,
}

impl Codepage {
    /// The byte → character map for this code page.
    pub fn table(self) -> &'static [char; 256] {
        match self {
            Codepage::Cp037 => &CP037,
            Codepage::Cp273 => &CP273,
            Codepage::Cp500 => &CP500,
            Codepage::Cp1140 => &CP1140,
        }
    }

    /// Append `bytes`, translated, to `out`.
    pub fn decode_into(self, bytes: &[u8], out: &mut String) {
        let table = self.table();
        out.extend(bytes.iter().map(|&b| table[b as usize]));
    }

    /// `bytes` translated to an owned `String`.
    pub fn decode(self, bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len());
        self.decode_into(bytes, &mut out);
        out
    }
}

/// IBM-037 byte → character map.
static CP037: [char; 256] = [
    '\u{0}', '\u{1}', '\u{2}', '\u{3}', '\u{9c}', '\u{9}', '\u{86}', '\u{7f}', '\u{97}', '\u{8d}',
    '\u{8e}', '\u{b}', '\u{c}', '\u{d}', '\u{e}', '\u{f}', '\u{10}', '\u{11}', '\u{12}', '\u{13}',
    '\u{9d}', '\u{85}', '\u{8}', '\u{87}', '\u{18}', '\u{19}', '\u{92}', '\u{8f}', '\u{1c}',
    '\u{1d}', '\u{1e}', '\u{1f}', '\u{80}', '\u{81}', '\u{82}', '\u{83}', '\u{84}', '\u{a}',
    '\u{17}', '\u{1b}', '\u{88}', '\u{89}', '\u{8a}', '\u{8b}', '\u{8c}', '\u{5}', '\u{6}',
    '\u{7}', '\u{90}', '\u{91}', '\u{16}', '\u{93}', '\u{94}', '\u{95}', '\u{96}', '\u{4}',
    '\u{98}', '\u{99}', '\u{9a}', '\u{9b}', '\u{14}', '\u{15}', '\u{9e}', '\u{1a}', ' ', '\u{a0}',
    '\u{e2}', '\u{e4}', '\u{e0}', '\u{e1}', '\u{e3}', '\u{e5}', '\u{e7}', '\u{f1}', '\u{a2}', '.',
    '<', '(', '+', '|', '&', '\u{e9}', '\u{ea}', '\u{eb}', '\u{e8}', '\u{ed}', '\u{ee}', '\u{ef}',
    '\u{ec}', '\u{df}', '!', '$', '*', ')', ';', '\u{ac}', '-', '/', '\u{c2}', '\u{c4}', '\u{c0}',
    '\u{c1}', '\u{c3}', '\u{c5}', '\u{c7}', '\u{d1}', '\u{a6}', ',', '%', '_', '>', '?', '\u{f8}',
    '\u{c9}', '\u{ca}', '\u{cb}', '\u{c8}', '\u{cd}', '\u{ce}', '\u{cf}', '\u{cc}', '`', ':', '#',
    '@', '\u{27}', '=', '"', '\u{d8}', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', '\u{ab}',
    '\u{bb}', '\u{f0}', '\u{fd}', '\u{fe}', '\u{b1}', '\u{b0}', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
    'q', 'r', '\u{aa}', '\u{ba}', '\u{e6}', '\u{b8}', '\u{c6}', '\u{a4}', '\u{b5}', '~', 's', 't',
    'u', 'v', 'w', 'x', 'y', 'z', '\u{a1}', '\u{bf}', '\u{d0}', '\u{dd}', '\u{de}', '\u{ae}', '^',
    '\u{a3}', '\u{a5}', '\u{b7}', '\u{a9}', '\u{a7}', '\u{b6}', '\u{bc}', '\u{bd}', '\u{be}', '[',
    ']', '\u{af}', '\u{a8}', '\u{b4}', '\u{d7}', '{', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
    '\u{ad}', '\u{f4}', '\u{f6}', '\u{f2}', '\u{f3}', '\u{f5}', '}', 'J', 'K', 'L', 'M', 'N', 'O',
    'P', 'Q', 'R', '\u{b9}', '\u{fb}', '\u{fc}', '\u{f9}', '\u{fa}', '\u{ff}', '\u{5c}', '\u{f7}',
    'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '\u{b2}', '\u{d4}', '\u{d6}', '\u{d2}', '\u{d3}',
    '\u{d5}', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\u{b3}', '\u{db}', '\u{dc}',
    '\u{d9}', '\u{da}', '\u{9f}',
];

/// IBM-273 byte → character map.
static CP273: [char; 256] = [
    '\u{0}', '\u{1}', '\u{2}', '\u{3}', '\u{9c}', '\u{9}', '\u{86}', '\u{7f}', '\u{97}', '\u{8d}',
    '\u{8e}', '\u{b}', '\u{c}', '\u{d}', '\u{e}', '\u{f}', '\u{10}', '\u{11}', '\u{12}', '\u{13}',
    '\u{9d}', '\u{85}', '\u{8}', '\u{87}', '\u{18}', '\u{19}', '\u{92}', '\u{8f}', '\u{1c}',
    '\u{1d}', '\u{1e}', '\u{1f}', '\u{80}', '\u{81}', '\u{82}', '\u{83}', '\u{84}', '\u{a}',
    '\u{17}', '\u{1b}', '\u{88}', '\u{89}', '\u{8a}', '\u{8b}', '\u{8c}', '\u{5}', '\u{6}',
    '\u{7}', '\u{90}', '\u{91}', '\u{16}', '\u{93}', '\u{94}', '\u{95}', '\u{96}', '\u{4}',
    '\u{98}', '\u{99}', '\u{9a}', '\u{9b}', '\u{14}', '\u{15}', '\u{9e}', '\u{1a}', ' ', '\u{a0}',
    '\u{e2}', '{', '\u{e0}', '\u{e1}', '\u{e3}', '\u{e5}', '\u{e7}', '\u{f1}', '\u{c4}', '.', '<',
    '(', '+', '!', '&', '\u{e9}', '\u{ea}', '\u{eb}', '\u{e8}', '\u{ed}', '\u{ee}', '\u{ef}',
    '\u{ec}', '~', '\u{dc}', '$', '*', ')', ';', '^', '-', '/', '\u{c2}', '[', '\u{c0}', '\u{c1}',
    '\u{c3}', '\u{c5}', '\u{c7}', '\u{d1}', '\u{f6}', ',', '%', '_', '>', '?', '\u{f8}', '\u{c9}',
    '\u{ca}', '\u{cb}', '\u{c8}', '\u{cd}', '\u{ce}', '\u{cf}', '\u{cc}', '`', ':', '#', '\u{a7}',
    '\u{27}', '=', '"', '\u{d8}', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', '\u{ab}', '\u{bb}',
    '\u{f0}', '\u{fd}', '\u{fe}', '\u{b1}', '\u{b0}', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
    '\u{aa}', '\u{ba}', '\u{e6}', '\u{b8}', '\u{c6}', '\u{a4}', '\u{b5}', '\u{df}', 's', 't', 'u',
    'v', 'w', 'x', 'y', 'z', '\u{a1}', '\u{bf}', '\u{d0}', '\u{dd}', '\u{de}', '\u{ae}', '\u{a2}',
    '\u{a3}', '\u{a5}', '\u{b7}', '\u{a9}', '@', '\u{b6}', '\u{bc}', '\u{bd}', '\u{be}', '\u{ac}',
    '|', '\u{203e}', '\u{a8}', '\u{b4}', '\u{d7}', '\u{e4}', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
    'H', 'I', '\u{ad}', '\u{f4}', '\u{a6}', '\u{f2}', '\u{f3}', '\u{f5}', '\u{fc}', 'J', 'K', 'L',
    'M', 'N', 'O', 'P', 'Q', 'R', '\u{b9}', '\u{fb}', '}', '\u{f9}', '\u{fa}', '\u{ff}', '\u{d6}',
    '\u{f7}', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '\u{b2}', '\u{d4}', '\u{5c}', '\u{d2}',
    '\u{d3}', '\u{d5}', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\u{b3}', '\u{db}', ']',
    '\u{d9}', '\u{da}', '\u{9f}',
];

/// IBM-500 byte → character map.
static CP500: [char; 256] = [
    '\u{0}', '\u{1}', '\u{2}', '\u{3}', '\u{9c}', '\u{9}', '\u{86}', '\u{7f}', '\u{97}', '\u{8d}',
    '\u{8e}', '\u{b}', '\u{c}', '\u{d}', '\u{e}', '\u{f}', '\u{10}', '\u{11}', '\u{12}', '\u{13}',
    '\u{9d}', '\u{85}', '\u{8}', '\u{87}', '\u{18}', '\u{19}', '\u{92}', '\u{8f}', '\u{1c}',
    '\u{1d}', '\u{1e}', '\u{1f}', '\u{80}', '\u{81}', '\u{82}', '\u{83}', '\u{84}', '\u{a}',
    '\u{17}', '\u{1b}', '\u{88}', '\u{89}', '\u{8a}', '\u{8b}', '\u{8c}', '\u{5}', '\u{6}',
    '\u{7}', '\u{90}', '\u{91}', '\u{16}', '\u{93}', '\u{94}', '\u{95}', '\u{96}', '\u{4}',
    '\u{98}', '\u{99}', '\u{9a}', '\u{9b}', '\u{14}', '\u{15}', '\u{9e}', '\u{1a}', ' ', '\u{a0}',
    '\u{e2}', '\u{e4}', '\u{e0}', '\u{e1}', '\u{e3}', '\u{e5}', '\u{e7}', '\u{f1}', '[', '.', '<',
    '(', '+', '!', '&', '\u{e9}', '\u{ea}', '\u{eb}', '\u{e8}', '\u{ed}', '\u{ee}', '\u{ef}',
    '\u{ec}', '\u{df}', ']', '$', '*', ')', ';', '^', '-', '/', '\u{c2}', '\u{c4}', '\u{c0}',
    '\u{c1}', '\u{c3}', '\u{c5}', '\u{c7}', '\u{d1}', '\u{a6}', ',', '%', '_', '>', '?', '\u{f8}',
    '\u{c9}', '\u{ca}', '\u{cb}', '\u{c8}', '\u{cd}', '\u{ce}', '\u{cf}', '\u{cc}', '`', ':', '#',
    '@', '\u{27}', '=', '"', '\u{d8}', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', '\u{ab}',
    '\u{bb}', '\u{f0}', '\u{fd}', '\u{fe}', '\u{b1}', '\u{b0}', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
    'q', 'r', '\u{aa}', '\u{ba}', '\u{e6}', '\u{b8}', '\u{c6}', '\u{a4}', '\u{b5}', '~', 's', 't',
    'u', 'v', 'w', 'x', 'y', 'z', '\u{a1}', '\u{bf}', '\u{d0}', '\u{dd}', '\u{de}', '\u{ae}',
    '\u{a2}', '\u{a3}', '\u{a5}', '\u{b7}', '\u{a9}', '\u{a7}', '\u{b6}', '\u{bc}', '\u{bd}',
    '\u{be}', '\u{ac}', '|', '\u{af}', '\u{a8}', '\u{b4}', '\u{d7}', '{', 'A', 'B', 'C', 'D', 'E',
    'F', 'G', 'H', 'I', '\u{ad}', '\u{f4}', '\u{f6}', '\u{f2}', '\u{f3}', '\u{f5}', '}', 'J', 'K',
    'L', 'M', 'N', 'O', 'P', 'Q', 'R', '\u{b9}', '\u{fb}', '\u{fc}', '\u{f9}', '\u{fa}', '\u{ff}',
    '\u{5c}', '\u{f7}', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '\u{b2}', '\u{d4}', '\u{d6}',
    '\u{d2}', '\u{d3}', '\u{d5}', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\u{b3}',
    '\u{db}', '\u{dc}', '\u{d9}', '\u{da}', '\u{9f}',
];

/// IBM-1140 byte → character map.
static CP1140: [char; 256] = [
    '\u{0}', '\u{1}', '\u{2}', '\u{3}', '\u{9c}', '\u{9}', '\u{86}', '\u{7f}', '\u{97}', '\u{8d}',
    '\u{8e}', '\u{b}', '\u{c}', '\u{d}', '\u{e}', '\u{f}', '\u{10}', '\u{11}', '\u{12}', '\u{13}',
    '\u{9d}', '\u{85}', '\u{8}', '\u{87}', '\u{18}', '\u{19}', '\u{92}', '\u{8f}', '\u{1c}',
    '\u{1d}', '\u{1e}', '\u{1f}', '\u{80}', '\u{81}', '\u{82}', '\u{83}', '\u{84}', '\u{a}',
    '\u{17}', '\u{1b}', '\u{88}', '\u{89}', '\u{8a}', '\u{8b}', '\u{8c}', '\u{5}', '\u{6}',
    '\u{7}', '\u{90}', '\u{91}', '\u{16}', '\u{93}', '\u{94}', '\u{95}', '\u{96}', '\u{4}',
    '\u{98}', '\u{99}', '\u{9a}', '\u{9b}', '\u{14}', '\u{15}', '\u{9e}', '\u{1a}', ' ', '\u{a0}',
    '\u{e2}', '\u{e4}', '\u{e0}', '\u{e1}', '\u{e3}', '\u{e5}', '\u{e7}', '\u{f1}', '\u{a2}', '.',
    '<', '(', '+', '|', '&', '\u{e9}', '\u{ea}', '\u{eb}', '\u{e8}', '\u{ed}', '\u{ee}', '\u{ef}',
    '\u{ec}', '\u{df}', '!', '$', '*', ')', ';', '\u{ac}', '-', '/', '\u{c2}', '\u{c4}', '\u{c0}',
    '\u{c1}', '\u{c3}', '\u{c5}', '\u{c7}', '\u{d1}', '\u{a6}', ',', '%', '_', '>', '?', '\u{f8}',
    '\u{c9}', '\u{ca}', '\u{cb}', '\u{c8}', '\u{cd}', '\u{ce}', '\u{cf}', '\u{cc}', '`', ':', '#',
    '@', '\u{27}', '=', '"', '\u{d8}', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', '\u{ab}',
    '\u{bb}', '\u{f0}', '\u{fd}', '\u{fe}', '\u{b1}', '\u{b0}', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
    'q', 'r', '\u{aa}', '\u{ba}', '\u{e6}', '\u{b8}', '\u{c6}', '\u{20ac}', '\u{b5}', '~', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', '\u{a1}', '\u{bf}', '\u{d0}', '\u{dd}', '\u{de}', '\u{ae}',
    '^', '\u{a3}', '\u{a5}', '\u{b7}', '\u{a9}', '\u{a7}', '\u{b6}', '\u{bc}', '\u{bd}', '\u{be}',
    '[', ']', '\u{af}', '\u{a8}', '\u{b4}', '\u{d7}', '{', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
    'I', '\u{ad}', '\u{f4}', '\u{f6}', '\u{f2}', '\u{f3}', '\u{f5}', '}', 'J', 'K', 'L', 'M', 'N',
    'O', 'P', 'Q', 'R', '\u{b9}', '\u{fb}', '\u{fc}', '\u{f9}', '\u{fa}', '\u{ff}', '\u{5c}',
    '\u{f7}', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '\u{b2}', '\u{d4}', '\u{d6}', '\u{d2}',
    '\u{d3}', '\u{d5}', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '\u{b3}', '\u{db}',
    '\u{dc}', '\u{d9}', '\u{da}', '\u{9f}',
];

#[cfg(test)]
mod test {
    use super::*;

    /// Spot checks against the CPython codecs the tables were generated from.
    /// One invariant character, one page-specific divergence each.
    #[test]
    fn every_table_decodes_its_own_page() {
        let cases = [
            (Codepage::Cp037, 0x4A, '\u{A2}'), // cent
            (Codepage::Cp273, 0x4A, '\u{C4}'), // A-umlaut
            (Codepage::Cp500, 0x4A, '['),
            (Codepage::Cp1140, 0x9F, '\u{20AC}'), // euro, the one 037 lacks
        ];
        for (page, byte, expected) in cases {
            assert_eq!(page.table()[byte], expected, "{page:?} 0x{byte:02X}");
        }
    }

    #[test]
    fn the_invariant_characters_agree_across_pages() {
        // A-Z, 0-9 and space sit at the same bytes on every page; a table
        // built from the wrong file would show up here.
        for page in [
            Codepage::Cp037,
            Codepage::Cp273,
            Codepage::Cp500,
            Codepage::Cp1140,
        ] {
            let table = page.table();
            assert_eq!(table[0x40], ' ', "{page:?}");
            assert_eq!(table[0xC1], 'A', "{page:?}");
            assert_eq!(table[0xE9], 'Z', "{page:?}");
            assert_eq!(table[0xF0], '0', "{page:?}");
            assert_eq!(table[0xF9], '9', "{page:?}");
        }
    }

    #[test]
    fn cp1140_is_cp037_with_the_euro() {
        // The pages differ at exactly one byte, which is what makes 1140 the
        // safe default swap for a euro-era shop.
        let differences: Vec<usize> = (0..256).filter(|&b| CP037[b] != CP1140[b]).collect();
        assert_eq!(differences, vec![0x9F]);
    }

    #[test]
    fn a_page_is_named_by_exactly_one_spelling() {
        // Deserialized by name, so the wire accepts these four and nothing
        // else -- which is what the connector validates against.
        for (name, page) in [
            ("cp037", Codepage::Cp037),
            ("cp273", Codepage::Cp273),
            ("cp500", Codepage::Cp500),
            ("cp1140", Codepage::Cp1140),
        ] {
            let parsed: Codepage = serde_yaml::from_str(name).expect(name);
            assert_eq!(parsed, page, "{name}");
        }
        for name in ["CP037", "037", "ibm037", "cp1047", "utf-8"] {
            assert!(
                serde_yaml::from_str::<Codepage>(name).is_err(),
                "{name} must not parse"
            );
        }
    }

    #[test]
    fn decoding_never_fails() {
        // Complete single-byte maps, so every byte has a character and a
        // record full of arbitrary bytes still yields a string.
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(Codepage::Cp037.decode(&all).chars().count(), 256);
    }
}
