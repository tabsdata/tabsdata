//
// Copyright 2026 Tabs Data Inc.
//

//! Shared request types for the time-bucketed metrics and Sankey endpoints.
//!
//! [`WindowSpec`] resolves to a `[start_ms, end_ms)` interval whose
//! right edge is [`WindowSpec::from`] (defaulting to server-now). Two
//! alignment modes (`Calendar`, `Rolling`) share the same parameter
//! set; the mode only controls how the start is computed. The window's
//! `unit` is also the GROUP BY grain — `span` determines the number of
//! rows returned (`unit=hour&span=24` → 24 hourly rows).
//!
//! [`DimensionFilter`] is the include/exclude semantic shared by
//! `/metrics` / `/inflight` / `/sankey`. `exclude` wins on overlap.

use crate::types::basic::EpochMs;
use chrono::Utc;
use td_common::bucket;
use td_error::td_error;

#[ta_objects::dxo]
mod definitions {
    /// Calendar unit driving both the window grammar (width +
    /// right-edge snap) and the aggregation grain (one row per unit).
    /// `Quarter` and `Year` are not stored as precomputed columns;
    /// the SQL groups by `ended_on_month_ms` and a Rust post-pass folds
    /// the monthly rows up.
    #[ta_objects::typed_enum]
    #[derive(Eq, PartialEq, Hash, Copy)]
    pub enum WindowUnit {
        Hour,
        Day,
        Week,
        Month,
        Quarter,
        Year,
    }

    /// How the start of the window is computed from `from`.
    ///
    /// `Calendar` snaps the start to the calendar-unit boundary
    /// containing `from`, with the end at `from` itself. Reporting-style
    /// views — "week-to-date", "month-to-date", "the last 3 quarters
    /// ending today". `span > 1` extends back `(span − 1)` additional
    /// full units before the snapped boundary.
    ///
    /// `Rolling` is a fixed-width window of exactly `span * unit`
    /// ending at `from` — for sliding views where the boundaries don't
    /// snap to a calendar ("trailing 7 days", "last 24 hours",
    /// Tuesday-to-Tuesday weeks pinned to a non-ISO cadence).
    #[ta_objects::typed_enum]
    #[derive(Eq, PartialEq, Hash, Copy)]
    pub enum WindowAlign {
        Calendar,
        Rolling,
    }
}

pub use definitions::*;

impl From<WindowUnit> for bucket::WindowUnit {
    fn from(u: WindowUnit) -> Self {
        match u {
            WindowUnit::Hour => bucket::WindowUnit::Hour,
            WindowUnit::Day => bucket::WindowUnit::Day,
            WindowUnit::Week => bucket::WindowUnit::Week,
            WindowUnit::Month => bucket::WindowUnit::Month,
            WindowUnit::Quarter => bucket::WindowUnit::Quarter,
            WindowUnit::Year => bucket::WindowUnit::Year,
        }
    }
}

impl WindowUnit {
    /// Maximum span (in units) allowed for a single window. Guards
    /// against accidental "all-time" requests; tuned to keep the
    /// per-request row count bounded.
    pub fn max_span(&self) -> u32 {
        match self {
            WindowUnit::Hour => 168,   // 1 week
            WindowUnit::Day => 90,     // ~3 months
            WindowUnit::Week => 52,    // 1 year
            WindowUnit::Month => 60,   // 5 years
            WindowUnit::Quarter => 20, // 5 years
            WindowUnit::Year => 10,
        }
    }
}

#[td_error]
pub enum TimeWindowError {
    #[error("end_ms ({end_ms}) must be greater than start_ms ({start_ms})")]
    EmptyWindow { start_ms: i64, end_ms: i64 } = 0,
    #[error("span {span} exceeds cap {cap} for unit {unit}")]
    SpanTooLarge { unit: String, span: u32, cap: u32 } = 1,
    #[error("span must be >= 1")]
    ZeroSpan = 2,
    #[error(
        "gap-filled response would have {rows} rows \
         ({buckets} buckets × {dims} dims × {statuses} statuses), \
         exceeds cap {cap}; narrow the window or apply filters"
    )]
    TooManyGapFilledRows {
        rows: usize,
        buckets: usize,
        dims: usize,
        statuses: usize,
        cap: usize,
    } = 3,
}

/// Maximum number of rows a gap-filled `/metrics` response may carry.
/// Bounds the worst-case `buckets × dims × statuses` cross-product.
pub const MAX_GAP_FILLED_ROWS: usize = 50_000;

/// Walk calendar-aligned bucket boundaries in `[start_ms, end_ms)` at
/// `unit` grain — every `floor(t, unit)` value that any `t` in the
/// window could collapse to under `GROUP BY ended_on_<unit>_ms`.
///
/// For calendar-aligned windows (`start_ms == floor(start_ms, unit)`)
/// this returns exactly `span` buckets. For rolling-aligned windows
/// starting mid-unit, the first bucket is `floor(start_ms, unit) <
/// start_ms` and represents the partial leading slice; the count can
/// be `span + 1`.
pub fn expected_buckets(start_ms: i64, end_ms: i64, unit: WindowUnit) -> Vec<i64> {
    let u: bucket::WindowUnit = unit.into();
    let mut b = bucket::floor_unit_ms(u, start_ms);
    let mut out = Vec::with_capacity(unit.max_span() as usize + 1);
    while b < end_ms {
        out.push(b);
        b = bucket::add_units_ms(u, b, 1);
    }
    out
}

/// A resolved `[start_ms, end_ms)` interval. `start_ms` inclusive,
/// `end_ms` exclusive. Produced by [`WindowSpec::resolve`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TimeWindow {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl TimeWindow {
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    /// Reject empty / inverted windows.
    pub fn validate(&self) -> Result<(), TimeWindowError> {
        if self.end_ms <= self.start_ms {
            return Err(TimeWindowError::EmptyWindow {
                start_ms: self.start_ms,
                end_ms: self.end_ms,
            });
        }
        Ok(())
    }
}

/// Declarative window resolved into a [`TimeWindow`] by
/// [`WindowSpec::resolve`]. The right edge is `from` (defaulting to
/// server-now); [`WindowAlign`] picks how the start is computed.
///
/// ```text
/// from_ms  = self.from.unwrap_or_else(|| Utc::now())
/// end_ms   = from_ms
/// start_ms = match align:
///     Rolling  => from_ms - span * unit
///     Calendar => floor_unit(unit, from_ms) - (span - 1) * unit
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct WindowSpec {
    pub align: WindowAlign,
    /// Right edge of the window (UTC epoch ms, exclusive). `None` ⇒
    /// `Utc::now()` at resolution time. Callers shift the window into
    /// the past by passing a past instant directly.
    pub from: Option<EpochMs>,
    pub unit: WindowUnit,
    /// Window width in units. Must be >= 1.
    pub span: u32,
}

impl WindowSpec {
    pub fn validate(&self) -> Result<(), TimeWindowError> {
        if self.span == 0 {
            return Err(TimeWindowError::ZeroSpan);
        }
        let cap = self.unit.max_span();
        if self.span > cap {
            return Err(TimeWindowError::SpanTooLarge {
                unit: self.unit.to_string(),
                span: self.span,
                cap,
            });
        }
        Ok(())
    }

    pub fn resolve(&self) -> Result<TimeWindow, TimeWindowError> {
        let from_ms = self
            .from
            .map(|f| *f)
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        self.resolve_at(from_ms)
    }

    /// Resolution with an explicit `from` — kept `pub(crate)` for
    /// deterministic tests. Production callers go through [`resolve`],
    /// which sources `from` from [`Self::from`] when set and from
    /// `Utc::now()` otherwise.
    pub(crate) fn resolve_at(&self, from_ms: i64) -> Result<TimeWindow, TimeWindowError> {
        self.validate()?;
        let u: bucket::WindowUnit = self.unit.into();
        let end_ms = from_ms;
        let start_ms = match self.align {
            WindowAlign::Rolling => bucket::add_units_ms(u, end_ms, -(self.span as i64)),
            WindowAlign::Calendar => bucket::add_units_ms(
                u,
                bucket::floor_unit_ms(u, end_ms),
                -((self.span as i64) - 1),
            ),
        };
        let w = TimeWindow { start_ms, end_ms };
        w.validate()?;
        Ok(w)
    }
}

/// Include / exclude lists over a dimension.
///
/// * `include` empty ⇒ "any value is allowed in"
/// * `exclude` empty ⇒ "no value is excluded"
/// * If both contain a value, `exclude` wins.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct DimensionFilter<Id> {
    pub include: Vec<Id>,
    pub exclude: Vec<Id>,
}

impl<Id: Eq> DimensionFilter<Id> {
    pub fn new(include: Vec<Id>, exclude: Vec<Id>) -> Self {
        Self { include, exclude }
    }

    /// Whether `id` survives the filter.
    pub fn matches(&self, id: &Id) -> bool {
        if self.exclude.iter().any(|x| x == id) {
            return false;
        }
        if self.include.is_empty() {
            return true;
        }
        self.include.iter().any(|x| x == id)
    }

    /// True if the filter has no constraints (everything passes).
    pub fn is_unconstrained(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ms(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .unwrap()
            .timestamp_millis()
    }

    // -------- Window resolution -----------------------------------------

    /// Reference `from` for every resolver case: Wed 2026-05-20 18:00 UTC.
    fn from_now() -> i64 {
        ms(2026, 5, 20, 18, 0, 0)
    }

    fn calendar(unit: WindowUnit, span: u32) -> WindowSpec {
        WindowSpec {
            align: WindowAlign::Calendar,
            from: None,
            unit,
            span,
        }
    }

    fn rolling(unit: WindowUnit, span: u32) -> WindowSpec {
        WindowSpec {
            align: WindowAlign::Rolling,
            from: None,
            unit,
            span,
        }
    }

    // -------- Calendar alignment: snap start, end = from ---------------

    /// `from = Wed 18:00, unit=week, span=1` → `[Mon 00:00, Wed 18:00)`
    /// (week-to-date as of `from`).
    #[test]
    fn calendar_week_to_date() {
        let w = calendar(WindowUnit::Week, 1)
            .resolve_at(from_now())
            .unwrap();
        assert_eq!(w.start_ms, ms(2026, 5, 18, 0, 0, 0)); // Mon of current week
        assert_eq!(w.end_ms, from_now());
    }

    /// `span=4` extends back 3 additional full weeks before the snapped
    /// week boundary. Mon 5-18 minus 3 weeks = Mon 4-27.
    #[test]
    fn calendar_span_4_weeks() {
        let w = calendar(WindowUnit::Week, 4)
            .resolve_at(from_now())
            .unwrap();
        assert_eq!(w.start_ms, ms(2026, 4, 27, 0, 0, 0));
        assert_eq!(w.end_ms, from_now());
    }

    #[test]
    fn calendar_month_to_date() {
        let w = calendar(WindowUnit::Month, 1)
            .resolve_at(from_now())
            .unwrap();
        assert_eq!(w.start_ms, ms(2026, 5, 1, 0, 0, 0));
        assert_eq!(w.end_ms, from_now());
    }

    #[test]
    fn calendar_quarter_to_date() {
        let w = calendar(WindowUnit::Quarter, 1)
            .resolve_at(from_now())
            .unwrap();
        // 2026-05 is in Q2 → Apr 1.
        assert_eq!(w.start_ms, ms(2026, 4, 1, 0, 0, 0));
        assert_eq!(w.end_ms, from_now());
    }

    #[test]
    fn calendar_year_to_date() {
        let w = calendar(WindowUnit::Year, 1)
            .resolve_at(from_now())
            .unwrap();
        assert_eq!(w.start_ms, ms(2026, 1, 1, 0, 0, 0));
        assert_eq!(w.end_ms, from_now());
    }

    // -------- Rolling alignment: end = from, start = end - span*unit --

    #[test]
    fn rolling_one_week_before_from() {
        let w = rolling(WindowUnit::Week, 1).resolve_at(from_now()).unwrap();
        assert_eq!(w.start_ms, ms(2026, 5, 13, 18, 0, 0));
        assert_eq!(w.end_ms, from_now());
    }

    #[test]
    fn rolling_seven_days() {
        let at = from_now() - 3_600_000;
        let w = rolling(WindowUnit::Day, 7).resolve_at(at).unwrap();
        assert_eq!(w.end_ms, at);
        assert_eq!(w.start_ms, at - 7 * 86_400_000);
    }

    #[test]
    fn rolling_year_is_calendar_aware() {
        // add_units_ms walks the calendar for year/month/quarter, so
        // rolling+year preserves day-of-year rather than subtracting 365 days.
        let w = rolling(WindowUnit::Year, 1).resolve_at(from_now()).unwrap();
        assert_eq!(w.start_ms, ms(2025, 5, 20, 18, 0, 0));
        assert_eq!(w.end_ms, from_now());
    }

    // -------- Explicit `from` ------------------------------------------

    /// Weeks pinned to Wednesday-to-Wednesday by passing a Wednesday
    /// midnight as `from` under `Rolling` alignment.
    #[test]
    fn from_pins_week_to_wednesday() {
        let wed_midnight = ms(2026, 5, 20, 0, 0, 0);
        let spec = WindowSpec {
            align: WindowAlign::Rolling,
            from: Some(EpochMs::try_from(wed_midnight).unwrap()),
            unit: WindowUnit::Week,
            span: 1,
        };
        // resolve() uses `from` when Some, ignoring Utc::now().
        let w = spec.resolve().unwrap();
        assert_eq!(w.end_ms, wed_midnight);
        assert_eq!(w.start_ms, ms(2026, 5, 13, 0, 0, 0));
    }

    /// `span=4` with the Wed anchor — every boundary lands on a Wed.
    #[test]
    fn from_four_wednesdays() {
        let wed = ms(2026, 5, 20, 0, 0, 0);
        let spec = WindowSpec {
            align: WindowAlign::Rolling,
            from: Some(EpochMs::try_from(wed).unwrap()),
            unit: WindowUnit::Week,
            span: 4,
        };
        let w = spec.resolve().unwrap();
        assert_eq!(w.end_ms, wed);
        assert_eq!(w.start_ms, ms(2026, 4, 22, 0, 0, 0)); // four weeks earlier, also a Wed
    }

    /// `resolve()` and `resolve_at(*from)` agree when `from` is Some.
    #[test]
    fn from_some_resolves_same_as_resolve_at() {
        let pinned = ms(2026, 5, 20, 12, 34, 56);
        let from_resolve_at = rolling(WindowUnit::Day, 1).resolve_at(pinned).unwrap();

        let from_resolve = WindowSpec {
            align: WindowAlign::Rolling,
            from: Some(EpochMs::try_from(pinned).unwrap()),
            unit: WindowUnit::Day,
            span: 1,
        }
        .resolve()
        .unwrap();
        assert_eq!(from_resolve, from_resolve_at);
    }

    /// `from` under `Calendar`: snap start to the calendar-unit
    /// boundary containing `from`, end = from.
    #[test]
    fn from_under_calendar_snaps_start() {
        let wed_evening = ms(2026, 5, 20, 18, 0, 0);
        let spec = WindowSpec {
            align: WindowAlign::Calendar,
            from: Some(EpochMs::try_from(wed_evening).unwrap()),
            unit: WindowUnit::Week,
            span: 1,
        };
        let w = spec.resolve().unwrap();
        assert_eq!(w.start_ms, ms(2026, 5, 18, 0, 0, 0)); // Mon of from's week
        assert_eq!(w.end_ms, wed_evening);
    }

    // -------- Validation ------------------------------------------------

    #[test]
    fn validate_rejects_zero_span() {
        let spec = WindowSpec {
            align: WindowAlign::Calendar,
            from: None,
            unit: WindowUnit::Day,
            span: 0,
        };
        assert!(matches!(
            spec.resolve_at(from_now()),
            Err(TimeWindowError::ZeroSpan)
        ));
    }

    #[test]
    fn validate_rejects_span_over_cap() {
        let spec = WindowSpec {
            align: WindowAlign::Calendar,
            from: None,
            unit: WindowUnit::Hour,
            span: WindowUnit::Hour.max_span() + 1,
        };
        assert!(matches!(
            spec.resolve_at(from_now()),
            Err(TimeWindowError::SpanTooLarge { .. })
        ));
    }

    /// `from = Mon 00:00, calendar+week+span=1` snaps start to that
    /// same Mon, producing a zero-length window — rejected.
    #[test]
    fn calendar_at_unit_boundary_is_empty_window() {
        let mon_midnight = ms(2026, 5, 18, 0, 0, 0);
        let spec = WindowSpec {
            align: WindowAlign::Calendar,
            from: Some(EpochMs::try_from(mon_midnight).unwrap()),
            unit: WindowUnit::Week,
            span: 1,
        };
        assert!(matches!(
            spec.resolve(),
            Err(TimeWindowError::EmptyWindow { .. })
        ));
    }

    #[test]
    fn time_window_rejects_empty_window() {
        let w = TimeWindow::new(1000, 1000);
        assert!(matches!(
            w.validate(),
            Err(TimeWindowError::EmptyWindow { .. })
        ));
        let w = TimeWindow::new(2000, 1000);
        assert!(matches!(
            w.validate(),
            Err(TimeWindowError::EmptyWindow { .. })
        ));
    }

    // -------- DimensionFilter -------------------------------------------

    #[test]
    fn dimension_filter_unconstrained_accepts_all() {
        let f = DimensionFilter::<i32>::default();
        assert!(f.is_unconstrained());
        assert!(f.matches(&1));
        assert!(f.matches(&42));
    }

    #[test]
    fn dimension_filter_include_only() {
        let f = DimensionFilter::new(vec![1, 2, 3], vec![]);
        assert!(f.matches(&1));
        assert!(f.matches(&2));
        assert!(!f.matches(&4));
    }

    #[test]
    fn dimension_filter_exclude_only() {
        let f = DimensionFilter::new(vec![], vec![5, 6]);
        assert!(f.matches(&1));
        assert!(!f.matches(&5));
        assert!(!f.matches(&6));
    }

    #[test]
    fn dimension_filter_exclude_wins_on_overlap() {
        let f = DimensionFilter::new(vec![1, 2, 3], vec![2]);
        assert!(f.matches(&1));
        assert!(!f.matches(&2));
        assert!(f.matches(&3));
        assert!(!f.matches(&4));
    }

    #[test]
    fn typed_enums_deserialize_lowercase_aliases() {
        // URL params can arrive lowercased (e.g. `?unit=year`); the
        // typed_enum macro emits a lowercase alias per variant.
        let s: WindowUnit = serde_json::from_str("\"year\"").unwrap();
        assert!(matches!(s, WindowUnit::Year));
        let s: WindowUnit = serde_json::from_str("\"month\"").unwrap();
        assert!(matches!(s, WindowUnit::Month));
        let s: WindowAlign = serde_json::from_str("\"calendar\"").unwrap();
        assert!(matches!(s, WindowAlign::Calendar));
        let s: WindowAlign = serde_json::from_str("\"rolling\"").unwrap();
        assert!(matches!(s, WindowAlign::Rolling));
        // Canonical PascalCase still works.
        let s: WindowUnit = serde_json::from_str("\"Year\"").unwrap();
        assert!(matches!(s, WindowUnit::Year));
    }
}
