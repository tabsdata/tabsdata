//
// Copyright 2025 Tabs Data Inc.
//

use data_encoding::BASE32HEX_NOPAD;
use std::hint::black_box;
use std::time::{Duration, Instant, UNIX_EPOCH};

fn main() {
    let n = 1_000_000_000;
    let start = Instant::now();
    let mut previous: Option<String> = None;
    let mut checksum: u64 = 0;
    for i in 0..n {
        let uuid = uuid7::uuid7();
        let code = BASE32HEX_NOPAD.encode(uuid.as_bytes());
        if i < 100 {
            let timestamp = timestamp(uuid.as_bytes());
            let datetime = UNIX_EPOCH + Duration::from_millis(timestamp);
            let datetime: chrono::DateTime<chrono::Utc> = datetime.into();
            println!(
                "{} - {} - {}",
                code,
                timestamp,
                datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            );
        }
        let bytes = code.as_bytes();
        if !bytes.is_empty() {
            checksum ^= bytes[0] as u64;
            checksum = checksum.rotate_left(5) ^ (bytes[bytes.len() - 1] as u64);
        }
        black_box(&code);

        if let Some(ref p) = previous {
            if code <= *p {
                eprintln!("⚠️ Violation at {i}: {code} <= {p}");
            }
        }
        previous = Some(code);
    }
    let elapsed = start.elapsed();
    println!("Generated {n} ids in {:?}", elapsed);
    println!("≈ {:.2} µs/id", elapsed.as_secs_f64() * 1e6 / n as f64);
    println!("Checksum: {checksum}");
}

#[inline]
fn timestamp(bytes: &[u8; 16]) -> u64 {
    ((bytes[0] as u64) << 40)
        | ((bytes[1] as u64) << 32)
        | ((bytes[2] as u64) << 24)
        | ((bytes[3] as u64) << 16)
        | ((bytes[4] as u64) << 8)
        | (bytes[5] as u64)
}

#[inline]
pub fn id() -> String {
    let u = uuid7::uuid7();
    BASE32HEX_NOPAD.encode(u.as_bytes())
}
