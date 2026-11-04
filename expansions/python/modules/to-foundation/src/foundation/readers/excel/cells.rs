//
// Copyright 2026 Tabsdata Inc.
//

use calamine::{DataRef, ExcelDateTime};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub(super) const MAX_EXACT_INT: f64 = 9_007_199_254_740_992.0;

pub(super) fn render(value: &DataRef) -> Option<String> {
    match value {
        DataRef::Empty => None,
        DataRef::Int(i) => Some(i.to_string()),
        DataRef::Float(f) => Some(f.to_string()),
        DataRef::String(s) => Some(s.clone()),
        DataRef::SharedString(s) => Some((*s).to_string()),
        DataRef::Bool(b) => Some(b.to_string()),
        DataRef::DateTime(dt) if dt.is_duration() => Some(dt.as_f64().to_string()),
        DataRef::DateTime(dt) => Some(
            naive_of(dt)
                .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                .unwrap_or_else(|| dt.as_f64().to_string()),
        ),
        DataRef::DateTimeIso(s) | DataRef::DurationIso(s) => Some(s.clone()),
        DataRef::Error(e) => Some(e.to_string()),
    }
}

pub(super) fn as_i64(value: f64) -> Option<i64> {
    (value.is_finite() && value.fract() == 0.0 && value.abs() < MAX_EXACT_INT)
        .then_some(value as i64)
}

pub(super) fn naive_of(datetime: &ExcelDateTime) -> Option<NaiveDateTime> {
    let (year, month, day, hour, minute, second, milli) = datetime.to_ymd_hms_milli();
    NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)?.and_hms_milli_opt(
        hour as u32,
        minute as u32,
        second as u32,
        milli as u32,
    )
}

pub(super) fn micros_of(datetime: &ExcelDateTime) -> Option<i64> {
    if datetime.is_duration() {
        return None;
    }
    naive_of(datetime).map(|d| d.and_utc().timestamp_micros())
}

pub(super) fn iso_micros(text: &str) -> Option<i64> {
    let naive = text.parse::<NaiveDateTime>().ok().or_else(|| {
        text.parse::<NaiveDate>()
            .ok()
            .map(|d| d.and_time(NaiveTime::MIN))
    })?;
    Some(naive.and_utc().timestamp_micros())
}
