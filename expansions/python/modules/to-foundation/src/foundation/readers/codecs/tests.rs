//
// Copyright 2026 Tabsdata Inc.
//

use super::*;
use std::io::{Cursor, Write};

const EXTENSIONS: [&str; 8] = [
    ".br", ".bz2", ".gz", ".lz4", ".xz", ".zlib", ".zst", ".zstd",
];

fn plain() -> Vec<u8> {
    (0..200_000u32)
        .flat_map(|value| format!("{value},row-{value}\n").into_bytes())
        .collect()
}

fn compressed(extension: &str, data: &[u8]) -> Vec<u8> {
    match extension {
        ".br" => {
            let mut out = Vec::new();
            {
                let mut writer = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                writer.write_all(data).unwrap();
            }
            out
        }
        ".bz2" => {
            let mut writer =
                bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        ".gz" => {
            let mut writer =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        ".lz4" => {
            let mut writer = lz4_flex::frame::FrameEncoder::new(Vec::new());
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        ".xz" => {
            let mut writer = liblzma::write::XzEncoder::new(Vec::new(), 6);
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        ".zlib" => {
            let mut writer =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        ".zst" | ".zstd" => zstd::encode_all(data, 3).unwrap(),
        other => panic!("no encoder for {other}"),
    }
}

fn decoded(extension: &str, data: Vec<u8>, size: Option<usize>) -> Vec<u8> {
    let mut decoder = decoder(Cursor::new(data), extension).unwrap().unwrap();
    let mut out = Vec::new();
    loop {
        let part = read_up_to(decoder.as_mut(), size).unwrap();
        if part.is_empty() {
            return out;
        }
        out.extend(part);
        if size.is_none() {
            return out;
        }
    }
}

#[test]
fn every_codec_decodes_what_it_encoded() {
    let data = plain();
    for extension in EXTENSIONS {
        assert_eq!(
            decoded(extension, compressed(extension, &data), None),
            data,
            "{extension}"
        );
    }
}

#[test]
fn reads_of_any_size_return_the_same_bytes() {
    let data = plain();
    for extension in EXTENSIONS {
        for size in [1, 7, 4096, 1 << 20] {
            assert_eq!(
                decoded(extension, compressed(extension, &data), Some(size)),
                data,
                "{extension} {size}"
            );
        }
    }
}

#[test]
fn a_read_returns_no_more_than_asked() {
    let data = plain();
    for extension in EXTENSIONS {
        let mut decoder = decoder(Cursor::new(compressed(extension, &data)), extension)
            .unwrap()
            .unwrap();
        assert_eq!(read_up_to(decoder.as_mut(), Some(10)).unwrap(), &data[..10]);
    }
}

#[test]
fn concatenated_streams_decode_whole() {
    let (first, second) = (b"first part\n".to_vec(), b"second part\n".to_vec());
    for extension in [".bz2", ".gz", ".lz4", ".xz", ".zst"] {
        let mut joined = compressed(extension, &first);
        joined.extend(compressed(extension, &second));
        let expected = [first.clone(), second.clone()].concat();
        assert_eq!(decoded(extension, joined, None), expected, "{extension}");
    }
}

#[test]
fn an_unknown_extension_has_no_decoder() {
    assert!(decoder(Cursor::new(Vec::new()), ".zip").unwrap().is_none());
}

#[test]
fn data_that_is_not_of_the_codec_fails_the_read() {
    for extension in EXTENSIONS {
        let opened = decoder(
            Cursor::new(b"plain text, not compressed".to_vec()),
            extension,
        );
        let failed = match opened {
            Err(_) => true,
            Ok(decoder) => read_up_to(decoder.unwrap().as_mut(), None).is_err(),
        };
        assert!(failed, "{extension}");
    }
}
