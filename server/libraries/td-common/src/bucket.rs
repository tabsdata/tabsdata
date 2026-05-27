//
// Copyright 2026 Tabs Data Inc.
//

//! UTC time-bucket floors for operational metrics.
//!
//! Bucket floors are UTC epoch ms truncated to the start of an `hour`, `day`,
//! `week`, `month`, `quarter`, or `year`. They are computed once at write time
//! (when a run reaches a terminal state) and stored alongside the row so that
//! read queries reduce to integer-equal joins on indexed columns — no
//! `date_trunc` or dialect-specific date math at query time.
//!
//! Only `hour`, `day`, `week`, and `month` are stored on the row. `quarter`
//! and `year` are derived from `month_ms` in Rust during post-aggregation.

use chrono::{DateTime, Datelike, NaiveDate, Utc};

/// Calendar unit used by the metrics/Sankey window grammar.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WindowUnit {
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;

/// 1970-01-05 00:00:00 UTC. The first Monday at or after the Unix epoch.
/// Used as the anchor for ISO-8601 week bucketing.
const WEEK_ANCHOR_MS: i64 = 4 * DAY_MS;

/// The four bucket floors stored on a fact row.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BucketFloors {
    pub hour_ms: i64,
    pub day_ms: i64,
    pub week_ms: i64,
    pub month_ms: i64,
}

impl BucketFloors {
    /// Compute the four stored bucket floors for the given UTC instant.
    pub fn from_utc(dt: DateTime<Utc>) -> Self {
        let ms = dt.timestamp_millis();
        Self {
            hour_ms: hour_floor_ms(ms),
            day_ms: day_floor_ms(ms),
            week_ms: week_floor_ms(ms),
            month_ms: month_floor_ms(dt),
        }
    }
}

/// Floor of `ms` to the start of its hour (UTC epoch ms).
pub fn hour_floor_ms(ms: i64) -> i64 {
    ms - ms.rem_euclid(HOUR_MS)
}

/// Floor of `ms` to the start of its day (UTC epoch ms).
pub fn day_floor_ms(ms: i64) -> i64 {
    ms - ms.rem_euclid(DAY_MS)
}

/// Floor of `ms` to the start of its ISO-8601 week (Monday 00:00:00 UTC).
pub fn week_floor_ms(ms: i64) -> i64 {
    ms - (ms - WEEK_ANCHOR_MS).rem_euclid(WEEK_MS)
}

/// Floor of `dt` to the start of its month (day 1, 00:00:00 UTC).
pub fn month_floor_ms(dt: DateTime<Utc>) -> i64 {
    let d = dt.date_naive();
    start_of_month(d.year(), d.month())
}

/// Floor of `dt` to the start of its calendar quarter (Jan/Apr/Jul/Oct 1, 00:00:00 UTC).
pub fn quarter_floor_ms(dt: DateTime<Utc>) -> i64 {
    let d = dt.date_naive();
    let q_start_month = ((d.month() - 1) / 3) * 3 + 1;
    start_of_month(d.year(), q_start_month)
}

/// Floor of `dt` to the start of its calendar year (Jan 1, 00:00:00 UTC).
pub fn year_floor_ms(dt: DateTime<Utc>) -> i64 {
    start_of_month(dt.year(), 1)
}

/// Quarter floor of a stored `month_ms`.
pub fn quarter_floor_of_month_ms(month_ms: i64) -> i64 {
    let dt = DateTime::<Utc>::from_timestamp_millis(month_ms)
        .expect("month_ms must be a valid stored bucket floor");
    quarter_floor_ms(dt)
}

/// Year floor of a stored month_ms.
pub fn year_floor_of_month_ms(month_ms: i64) -> i64 {
    let dt = DateTime::<Utc>::from_timestamp_millis(month_ms)
        .expect("month_ms must be a valid stored bucket floor");
    year_floor_ms(dt)
}

/// Floor `ms` to the start of its calendar unit (UTC epoch ms).
pub fn floor_unit_ms(unit: WindowUnit, ms: i64) -> i64 {
    match unit {
        WindowUnit::Hour => hour_floor_ms(ms),
        WindowUnit::Day => day_floor_ms(ms),
        WindowUnit::Week => week_floor_ms(ms),
        WindowUnit::Month => month_floor_ms(to_dt(ms)),
        WindowUnit::Quarter => quarter_floor_ms(to_dt(ms)),
        WindowUnit::Year => year_floor_ms(to_dt(ms)),
    }
}

/// Advance `ms` by `n` units (UTC, calendar-aware).
///
/// `Hour`/`Day`/`Week` are exact millisecond multiples. `Month` /
/// `Quarter` / `Year` preserve the day-of-month, clamping to the last
/// valid day of the target month (Jan 31 + 1 month → Feb 28 or 29;
/// Feb 29 + 1 year → Feb 28 on non-leap years). Hour/min/sec/ms are
/// preserved exactly across calendar-unit adds.
pub fn add_units_ms(unit: WindowUnit, ms: i64, n: i64) -> i64 {
    match unit {
        WindowUnit::Hour => ms + n * HOUR_MS,
        WindowUnit::Day => ms + n * DAY_MS,
        WindowUnit::Week => ms + n * WEEK_MS,
        WindowUnit::Month => add_months_dt(to_dt(ms), n).timestamp_millis(),
        WindowUnit::Quarter => add_months_dt(to_dt(ms), n * 3).timestamp_millis(),
        WindowUnit::Year => add_years_dt(to_dt(ms), n).timestamp_millis(),
    }
}

fn to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).expect("ms must be a valid UTC timestamp")
}

fn add_months_dt(dt: DateTime<Utc>, n: i64) -> DateTime<Utc> {
    let total = dt.year() as i64 * 12 + (dt.month() as i64 - 1) + n;
    let new_year = i32::try_from(total.div_euclid(12)).expect("year out of i32 range");
    let new_month = (total.rem_euclid(12) + 1) as u32;
    let new_day = dt.day().min(last_day_of_month(new_year, new_month));
    // Clear day to 1 before changing month/year to avoid invalid intermediate dates.
    dt.with_day(1)
        .and_then(|d| d.with_year(new_year))
        .and_then(|d| d.with_month(new_month))
        .and_then(|d| d.with_day(new_day))
        .expect("month arithmetic must yield a valid date")
}

fn add_years_dt(dt: DateTime<Utc>, n: i64) -> DateTime<Utc> {
    let new_year = i32::try_from(dt.year() as i64 + n).expect("year out of i32 range");
    let new_day = dt.day().min(last_day_of_month(new_year, dt.month()));
    dt.with_day(1)
        .and_then(|d| d.with_year(new_year))
        .and_then(|d| d.with_day(new_day))
        .expect("year arithmetic must yield a valid date")
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(ny, nm, 1)
        .expect("calendar date must exist")
        .pred_opt()
        .expect("date before year-min not representable")
        .day()
}

fn start_of_month(year: i32, month: u32) -> i64 {
    NaiveDate::from_ymd_opt(year, month, 1)
        .expect("calendar month must exist")
        .and_hms_opt(0, 0, 0)
        .expect("midnight always exists")
        .and_utc()
        .timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dt(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    fn floor(y: i32, mo: u32, d: u32, h: u32) -> i64 {
        dt(y, mo, d, h, 0, 0).timestamp_millis()
    }

    #[test]
    fn hour_floor_truncates_minutes_and_seconds() {
        let t = dt(2026, 3, 14, 9, 26, 53);
        assert_eq!(hour_floor_ms(t.timestamp_millis()), floor(2026, 3, 14, 9));
    }

    #[test]
    fn day_floor_truncates_to_midnight() {
        let t = dt(2026, 3, 14, 23, 59, 59);
        assert_eq!(day_floor_ms(t.timestamp_millis()), floor(2026, 3, 14, 0));
    }

    #[test]
    fn week_anchor_is_monday_1970_01_05() {
        // 1970-01-05 was a Monday. Sanity-check the constant.
        let monday = dt(1970, 1, 5, 0, 0, 0).timestamp_millis();
        assert_eq!(WEEK_ANCHOR_MS, monday);
    }

    #[test]
    fn week_floor_lands_on_monday() {
        // 2026-03-14 is a Saturday — week starts 2026-03-09 (Monday).
        let sat = dt(2026, 3, 14, 15, 0, 0).timestamp_millis();
        assert_eq!(week_floor_ms(sat), floor(2026, 3, 9, 0));

        // The anchor itself floors to itself.
        assert_eq!(week_floor_ms(WEEK_ANCHOR_MS), WEEK_ANCHOR_MS);

        // The day before the anchor (1970-01-04 Sunday) belongs to the
        // previous week (1969-12-29 Monday).
        let sun = dt(1970, 1, 4, 0, 0, 0).timestamp_millis();
        let prev_mon = dt(1969, 12, 29, 0, 0, 0).timestamp_millis();
        assert_eq!(week_floor_ms(sun), prev_mon);
    }

    #[test]
    fn week_floor_handles_year_boundary() {
        // 2026-01-02 is a Friday → week starts 2025-12-29 (Monday).
        let fri = dt(2026, 1, 2, 12, 0, 0);
        assert_eq!(
            week_floor_ms(fri.timestamp_millis()),
            floor(2025, 12, 29, 0)
        );
    }

    #[test]
    fn month_floor_lands_on_day_one() {
        let t = dt(2026, 3, 14, 9, 26, 53);
        assert_eq!(month_floor_ms(t), floor(2026, 3, 1, 0));
        // Last instant of February → still February's floor.
        let feb_end = dt(2024, 2, 29, 23, 59, 59);
        assert_eq!(month_floor_ms(feb_end), floor(2024, 2, 1, 0));
    }

    #[test]
    fn quarter_floor_maps_months_to_q_start() {
        let cases = [
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 4),
            (5, 4),
            (6, 4),
            (7, 7),
            (8, 7),
            (9, 7),
            (10, 10),
            (11, 10),
            (12, 10),
        ];
        for (m, q) in cases {
            let t = dt(2026, m, 15, 12, 0, 0);
            assert_eq!(quarter_floor_ms(t), floor(2026, q, 1, 0), "month {m}");
        }
    }

    #[test]
    fn year_floor_strips_to_jan_one() {
        let t = dt(2026, 7, 4, 16, 0, 0);
        assert_eq!(year_floor_ms(t), floor(2026, 1, 1, 0));
    }

    #[test]
    fn from_utc_bundles_all_four_floors() {
        let t = dt(2026, 3, 14, 9, 26, 53);
        let f = BucketFloors::from_utc(t);
        assert_eq!(f.hour_ms, floor(2026, 3, 14, 9));
        assert_eq!(f.day_ms, floor(2026, 3, 14, 0));
        assert_eq!(f.week_ms, floor(2026, 3, 9, 0));
        assert_eq!(f.month_ms, floor(2026, 3, 1, 0));
    }

    #[test]
    fn floor_unit_dispatches_per_unit() {
        let t = dt(2026, 5, 20, 18, 26, 53).timestamp_millis();
        assert_eq!(floor_unit_ms(WindowUnit::Hour, t), floor(2026, 5, 20, 18));
        assert_eq!(floor_unit_ms(WindowUnit::Day, t), floor(2026, 5, 20, 0));
        assert_eq!(floor_unit_ms(WindowUnit::Week, t), floor(2026, 5, 18, 0));
        assert_eq!(floor_unit_ms(WindowUnit::Month, t), floor(2026, 5, 1, 0));
        assert_eq!(floor_unit_ms(WindowUnit::Quarter, t), floor(2026, 4, 1, 0));
        assert_eq!(floor_unit_ms(WindowUnit::Year, t), floor(2026, 1, 1, 0));
    }

    #[test]
    fn add_units_hour_day_week_are_exact_multiples() {
        let t = dt(2026, 5, 20, 18, 26, 53).timestamp_millis();
        assert_eq!(add_units_ms(WindowUnit::Hour, t, 1), t + HOUR_MS);
        assert_eq!(add_units_ms(WindowUnit::Hour, t, -3), t - 3 * HOUR_MS);
        assert_eq!(add_units_ms(WindowUnit::Day, t, 7), t + 7 * DAY_MS);
        assert_eq!(add_units_ms(WindowUnit::Week, t, -2), t - 2 * WEEK_MS);
    }

    #[test]
    fn add_months_clamps_to_last_day_when_target_is_shorter() {
        // Jan 31 + 1 month → Feb 28 (2026 is not a leap year).
        let jan31 = dt(2026, 1, 31, 12, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Month, jan31, 1),
            dt(2026, 2, 28, 12, 0, 0).timestamp_millis()
        );
        // Jan 31 + 1 month in a leap year → Feb 29.
        let jan31_leap = dt(2024, 1, 31, 12, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Month, jan31_leap, 1),
            dt(2024, 2, 29, 12, 0, 0).timestamp_millis()
        );
        // May 31 - 1 month → Apr 30.
        let may31 = dt(2026, 5, 31, 9, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Month, may31, -1),
            dt(2026, 4, 30, 9, 0, 0).timestamp_millis()
        );
    }

    #[test]
    fn add_months_crosses_year_boundary() {
        let dec15 = dt(2026, 12, 15, 0, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Month, dec15, 1),
            dt(2027, 1, 15, 0, 0, 0).timestamp_millis()
        );
        let jan10 = dt(2026, 1, 10, 0, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Month, jan10, -1),
            dt(2025, 12, 10, 0, 0, 0).timestamp_millis()
        );
        // 13 months forward = +1 year +1 month.
        assert_eq!(
            add_units_ms(WindowUnit::Month, jan10, 13),
            dt(2027, 2, 10, 0, 0, 0).timestamp_millis()
        );
    }

    #[test]
    fn add_quarter_is_three_months() {
        let mar15 = dt(2026, 3, 15, 0, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Quarter, mar15, 1),
            dt(2026, 6, 15, 0, 0, 0).timestamp_millis()
        );
        assert_eq!(
            add_units_ms(WindowUnit::Quarter, mar15, -1),
            dt(2025, 12, 15, 0, 0, 0).timestamp_millis()
        );
    }

    #[test]
    fn add_years_clamps_feb_29_on_non_leap_years() {
        let feb29 = dt(2024, 2, 29, 12, 0, 0).timestamp_millis();
        assert_eq!(
            add_units_ms(WindowUnit::Year, feb29, 1),
            dt(2025, 2, 28, 12, 0, 0).timestamp_millis()
        );
        // Two years lands on the next leap year, day preserved.
        assert_eq!(
            add_units_ms(WindowUnit::Year, feb29, 4),
            dt(2028, 2, 29, 12, 0, 0).timestamp_millis()
        );
    }

    #[test]
    fn add_units_preserves_sub_second_resolution() {
        // 2026-05-20 18:26:53.123 UTC.
        let t = dt(2026, 5, 20, 18, 26, 53).timestamp_millis() + 123;
        let plus_month = add_units_ms(WindowUnit::Month, t, 1);
        assert_eq!(plus_month % 1000, 123);
    }

    #[test]
    fn ms_helpers_round_trip_through_stored_month() {
        let t = dt(2026, 8, 20, 5, 0, 0);
        let month = month_floor_ms(t);
        assert_eq!(quarter_floor_of_month_ms(month), floor(2026, 7, 1, 0));
        assert_eq!(year_floor_of_month_ms(month), floor(2026, 1, 1, 0));
    }
}
