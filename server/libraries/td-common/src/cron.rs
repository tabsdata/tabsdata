//
// Copyright 2026 Tabsdata Inc.
//

use td_error::td_error;

#[td_error]
pub enum ScheduleError {
    #[error("Invalid cron schedule {schedule:?}: {cause}")]
    Invalid { schedule: String, cause: String } = 0,
}

struct Bounds {
    min: u64,
    max: u64,
    names: &'static [(&'static str, u64)],
}

const MINUTES: Bounds = Bounds {
    min: 0,
    max: 59,
    names: &[],
};

const HOURS: Bounds = Bounds {
    min: 0,
    max: 23,
    names: &[],
};

const DAYS_OF_MONTH: Bounds = Bounds {
    min: 1,
    max: 31,
    names: &[],
};

const MONTHS: Bounds = Bounds {
    min: 1,
    max: 12,
    names: &[
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("may", 5),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ],
};

const DAYS_OF_WEEK: Bounds = Bounds {
    min: 0,
    max: 6,
    names: &[
        ("sun", 0),
        ("mon", 1),
        ("tue", 2),
        ("wed", 3),
        ("thu", 4),
        ("fri", 5),
        ("sat", 6),
    ],
};

const FIELDS: [&Bounds; 5] = [&MINUTES, &HOURS, &DAYS_OF_MONTH, &MONTHS, &DAYS_OF_WEEK];

const DESCRIPTORS: [&str; 7] = [
    "@yearly",
    "@annually",
    "@monthly",
    "@weekly",
    "@daily",
    "@midnight",
    "@hourly",
];

pub const SCHEDULE_TIME_ZONE: &str = "Etc/UTC";

const EVERY: &str = "@every ";
const DESCRIPTOR_PREFIX: char = '@';
const TIME_ZONE_MARKER: &str = "TZ";
const TIME_ZONE_IN_SCHEDULE: &str =
    "cannot use TZ or CRON_TZ in schedule, use timeZone field instead";

const NANOSECOND: u64 = 1;
const MICROSECOND: u64 = 1_000 * NANOSECOND;
const MILLISECOND: u64 = 1_000 * MICROSECOND;
const SECOND: u64 = 1_000 * MILLISECOND;
const MINUTE: u64 = 60 * SECOND;
const HOUR: u64 = 60 * MINUTE;

const UNITS: [(&str, u64); 8] = [
    ("ns", NANOSECOND),
    ("us", MICROSECOND),
    ("\u{00b5}s", MICROSECOND),
    ("\u{03bc}s", MICROSECOND),
    ("ms", MILLISECOND),
    ("s", SECOND),
    ("m", MINUTE),
    ("h", HOUR),
];

const DURATION_LIMIT: u64 = 1 << 63;

pub fn validate_schedule(schedule: &str) -> Result<(), ScheduleError> {
    let checked = if schedule.contains(TIME_ZONE_MARKER) {
        Err(TIME_ZONE_IN_SCHEDULE.to_owned())
    } else {
        parse_standard(schedule)
    };
    checked.map_err(|cause| ScheduleError::Invalid {
        schedule: schedule.to_owned(),
        cause,
    })
}

fn parse_standard(spec: &str) -> Result<(), String> {
    if spec.is_empty() {
        return Err("empty spec string".to_owned());
    }
    if spec.starts_with(DESCRIPTOR_PREFIX) {
        return parse_descriptor(spec);
    }
    let fields: Vec<&str> = spec
        .split(char::is_whitespace)
        .filter(|f| !f.is_empty())
        .collect();
    if fields.len() != FIELDS.len() {
        return Err(format!(
            "expected exactly {} fields, found {}: [{}]",
            FIELDS.len(),
            fields.len(),
            fields.join(" ")
        ));
    }
    fields
        .iter()
        .zip(FIELDS)
        .try_for_each(|(field, bounds)| parse_field(field, bounds))
}

fn parse_field(field: &str, bounds: &Bounds) -> Result<(), String> {
    field
        .split(',')
        .filter(|range| !range.is_empty())
        .try_for_each(|range| parse_range(range, bounds))
}

fn parse_range(expr: &str, bounds: &Bounds) -> Result<(), String> {
    let range_and_step: Vec<&str> = expr.split('/').collect();
    let low_and_high: Vec<&str> = range_and_step[0].split('-').collect();
    let single = low_and_high.len() == 1;

    let (start, mut end) = if low_and_high[0] == "*" || low_and_high[0] == "?" {
        (bounds.min, bounds.max)
    } else {
        let start = parse_int_or_name(low_and_high[0], bounds)?;
        let end = match low_and_high.len() {
            1 => start,
            2 => parse_int_or_name(low_and_high[1], bounds)?,
            _ => return Err(format!("too many hyphens: {expr}")),
        };
        (start, end)
    };

    let step = match range_and_step.len() {
        1 => 1,
        2 => {
            let step = must_parse_int(range_and_step[1])?;
            if single {
                end = bounds.max;
            }
            step
        }
        _ => return Err(format!("too many slashes: {expr}")),
    };

    if start < bounds.min {
        return Err(format!(
            "beginning of range ({start}) below minimum ({}): {expr}",
            bounds.min
        ));
    }
    if end > bounds.max {
        return Err(format!(
            "end of range ({end}) above maximum ({}): {expr}",
            bounds.max
        ));
    }
    if start > end {
        return Err(format!(
            "beginning of range ({start}) beyond end of range ({end}): {expr}"
        ));
    }
    if step == 0 {
        return Err(format!("step of range should be a positive number: {expr}"));
    }
    Ok(())
}

fn parse_int_or_name(expr: &str, bounds: &Bounds) -> Result<u64, String> {
    let lowered = go_lowercase(expr);
    match bounds.names.iter().find(|(name, _)| *name == lowered) {
        Some((_, value)) => Ok(*value),
        None => must_parse_int(expr),
    }
}

fn go_lowercase(expr: &str) -> String {
    expr.chars()
        .filter_map(|c| c.to_lowercase().next())
        .collect()
}

fn must_parse_int(expr: &str) -> Result<u64, String> {
    let number: i64 = expr.parse().map_err(|error: std::num::ParseIntError| {
        let reason = match error.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                "value out of range"
            }
            _ => "invalid syntax",
        };
        format!("failed to parse int from {expr}: strconv.Atoi: parsing {expr:?}: {reason}")
    })?;
    u64::try_from(number).map_err(|_| format!("negative number ({number}) not allowed: {expr}"))
}

fn parse_descriptor(descriptor: &str) -> Result<(), String> {
    if DESCRIPTORS.contains(&descriptor) {
        return Ok(());
    }
    match descriptor.strip_prefix(EVERY) {
        Some(duration) => parse_duration(duration)
            .map_err(|error| format!("failed to parse duration {descriptor}: {error}")),
        None => Err(format!("unrecognized descriptor: {descriptor}")),
    }
}

fn parse_duration(original: &str) -> Result<(), String> {
    let invalid = || format!("time: invalid duration {}", quote(original.as_bytes()));
    let mut s = original.as_bytes();
    let mut total: u64 = 0;
    let mut negative = false;

    if let Some(&sign) = s.first()
        && (sign == b'-' || sign == b'+')
    {
        negative = sign == b'-';
        s = &s[1..];
    }
    if s == b"0" {
        return Ok(());
    }
    if s.is_empty() {
        return Err(invalid());
    }
    while !s.is_empty() {
        if !(s[0] == b'.' || s[0].is_ascii_digit()) {
            return Err(invalid());
        }
        let before = s.len();
        let (mut value, rest) = leading_int(s).ok_or_else(invalid)?;
        s = rest;
        let pre = before != s.len();

        let mut fraction = 0;
        let mut scale = 1.0;
        let mut post = false;
        if s.first() == Some(&b'.') {
            s = &s[1..];
            let before = s.len();
            (fraction, scale, s) = leading_fraction(s);
            post = before != s.len();
        }
        if !pre && !post {
            return Err(invalid());
        }

        let length = s
            .iter()
            .position(|&c| c == b'.' || c.is_ascii_digit())
            .unwrap_or(s.len());
        if length == 0 {
            return Err(format!(
                "time: missing unit in duration {}",
                quote(original.as_bytes())
            ));
        }
        let (name, rest) = s.split_at(length);
        s = rest;
        let unit = UNITS
            .iter()
            .find(|(unit, _)| unit.as_bytes() == name)
            .map(|(_, unit)| *unit)
            .ok_or_else(|| {
                format!(
                    "time: unknown unit {} in duration {}",
                    quote(name),
                    quote(original.as_bytes())
                )
            })?;
        if value > DURATION_LIMIT / unit {
            return Err(invalid());
        }
        value *= unit;
        if fraction > 0 {
            value += (fraction as f64 * (unit as f64 / scale)) as u64;
            if value > DURATION_LIMIT {
                return Err(invalid());
            }
        }
        total = total.wrapping_add(value);
        if total > DURATION_LIMIT {
            return Err(invalid());
        }
    }
    if !negative && total > DURATION_LIMIT - 1 {
        return Err(invalid());
    }
    Ok(())
}

fn leading_int(s: &[u8]) -> Option<(u64, &[u8])> {
    let mut value: u64 = 0;
    let mut length = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            break;
        }
        if value > DURATION_LIMIT / 10 {
            return None;
        }
        value = value * 10 + u64::from(c - b'0');
        if value > DURATION_LIMIT {
            return None;
        }
        length += 1;
    }
    Some((value, &s[length..]))
}

fn leading_fraction(s: &[u8]) -> (u64, f64, &[u8]) {
    let mut value: u64 = 0;
    let mut scale = 1.0;
    let mut overflow = false;
    let mut length = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            break;
        }
        length += 1;
        if overflow {
            continue;
        }
        if value > (DURATION_LIMIT - 1) / 10 {
            overflow = true;
            continue;
        }
        let next = value * 10 + u64::from(c - b'0');
        if next > DURATION_LIMIT {
            overflow = true;
            continue;
        }
        value = next;
        scale *= 10.0;
    }
    (value, scale, &s[length..])
}

fn quote(text: &[u8]) -> String {
    format!("\"{}\"", String::from_utf8_lossy(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tm_workspace::workspace_root;

    const SCHEDULES: &str = include_str!(concat!(
        workspace_root!(),
        "/server/libraries/td-common/resources/cron/schedules.jsonl"
    ));

    #[test]
    fn every_schedule_is_validated_as_robfig_and_kubernetes_validate_it() {
        let disagreements: Vec<String> = SCHEDULES
            .lines()
            .filter_map(|line| {
                let (schedule, valid): (String, bool) = serde_json::from_str(line).unwrap();
                (validate_schedule(&schedule).is_ok() != valid).then_some(line.to_owned())
            })
            .collect();
        assert_eq!(disagreements, Vec::<String>::new());
    }

    fn bounded(min: u64, max: u64) -> Bounds {
        Bounds {
            min,
            max,
            names: &[],
        }
    }

    fn accepts(schedule: &str) {
        if let Err(error) = validate_schedule(schedule) {
            panic!("{schedule:?} was refused: {error}");
        }
    }

    fn cause_of(schedule: &str) -> String {
        match validate_schedule(schedule) {
            Ok(()) => panic!("{schedule:?} was accepted"),
            Err(ScheduleError::Invalid { cause, .. }) => cause,
        }
    }

    #[test]
    fn ranges_parse_as_robfig_parses_them() {
        let valid = [
            ("5", 0, 7),
            ("0", 0, 7),
            ("7", 0, 7),
            ("5-5", 0, 7),
            ("5-6", 0, 7),
            ("5-7", 0, 7),
            ("5-6/2", 0, 7),
            ("5-7/2", 0, 7),
            ("5-7/1", 0, 7),
            ("*", 1, 3),
            ("*/2", 1, 3),
        ];
        for (expr, min, max) in valid {
            assert_eq!(parse_range(expr, &bounded(min, max)), Ok(()), "{expr}");
        }

        let invalid = [
            ("5--5", 0, 0, "too many hyphens"),
            ("jan-x", 0, 0, "failed to parse int from"),
            ("2-x", 1, 5, "failed to parse int from"),
            ("*/-12", 0, 0, "negative number"),
            ("*//2", 0, 0, "too many slashes"),
            ("1", 3, 5, "below minimum"),
            ("6", 3, 5, "above maximum"),
            ("5-3", 3, 5, "beyond end of range"),
            ("*/0", 0, 0, "should be a positive number"),
        ];
        for (expr, min, max, error) in invalid {
            let cause = parse_range(expr, &bounded(min, max)).unwrap_err();
            assert!(cause.contains(error), "{expr}: {cause}");
        }
    }

    #[test]
    fn fields_are_comma_separated_ranges() {
        for field in ["5", "5,6", "5,6,7", "1,5-7/2,3"] {
            assert_eq!(parse_field(field, &bounded(1, 7)), Ok(()), "{field}");
        }
    }

    #[test]
    fn standard_specs_parse_as_robfig_parses_them() {
        for schedule in ["5 * * * *", "@every 5m"] {
            accepts(schedule);
        }
        assert!(cause_of("5 j * * *").contains("failed to parse int from"));
        assert!(cause_of("* * * *").contains("expected exactly 5 fields"));
        assert!(cause_of("@every Xm").contains("failed to parse duration"));
        assert!(cause_of("@unrecognized").contains("unrecognized descriptor"));
        assert!(cause_of("").contains("empty spec string"));
    }

    #[test]
    fn kubernetes_accepts_these_schedules() {
        let accepted = [
            "* * * * *",
            "*/15 * * * *",
            "0 0 1 1 *",
            "0 9-17 * * mon-fri",
            "0 0 * JAN,Jul SUN",
            "0 0 * * fRi",
            "? * * * *",
            "*-5 * * * *",
            "1, * * * *",
            ", * * * *",
            "5/10 * * * *",
            "0 0 31 2 *",
            "\t0  0\u{00a0}* * *\n",
            "+5 * * * *",
            "@yearly",
            "@annually",
            "@monthly",
            "@weekly",
            "@daily",
            "@midnight",
            "@hourly",
            "@every 1h30m",
            "@every 1.5h",
            "@every .5s",
            "@every 0",
            "@every -5m",
            "@every 2\u{00b5}s",
            "@every 2\u{03bc}s",
        ];
        for schedule in accepted {
            accepts(schedule);
        }
    }

    #[test]
    fn kubernetes_refuses_these_schedules() {
        let refused = [
            ("0 0 * *", "expected exactly 5 fields"),
            ("0 0 * * * *", "expected exactly 5 fields"),
            ("60 0 * * *", "above maximum"),
            ("0 24 * * *", "above maximum"),
            ("0 0 0 * *", "below minimum"),
            ("0 0 * 13 *", "above maximum"),
            ("0 0 * * 7", "above maximum"),
            ("L * * * *", "failed to parse int from"),
            ("15W * * * *", "failed to parse int from"),
            ("0 0 * * 5#3", "failed to parse int from"),
            ("H * * * *", "failed to parse int from"),
            ("-5 * * * *", "failed to parse int from"),
            ("99999999999999999999 * * * *", "value out of range"),
            ("@YEARLY", "unrecognized descriptor"),
            ("@Daily", "unrecognized descriptor"),
            ("@daily ", "unrecognized descriptor"),
            (" @daily", "expected exactly 5 fields"),
            ("@reboot", "unrecognized descriptor"),
            ("@every", "unrecognized descriptor"),
            ("@every ", "invalid duration"),
            ("@every 5", "missing unit"),
            ("@every 5d", "unknown unit"),
            ("@every .s", "invalid duration"),
            ("@every 9223372036854775808ns", "invalid duration"),
            ("TZ=UTC 0 0 * * *", TIME_ZONE_IN_SCHEDULE),
            ("CRON_TZ=UTC 0 0 * * *", TIME_ZONE_IN_SCHEDULE),
        ];
        for (schedule, error) in refused {
            let cause = cause_of(schedule);
            assert!(cause.contains(error), "{schedule:?}: {cause}");
        }
    }

    #[test]
    fn durations_overflow_where_go_overflows() {
        assert_eq!(parse_duration("-9223372036854775808ns"), Ok(()));
        assert!(parse_duration("9223372036854775807ns").is_ok());
        assert!(parse_duration("9223372036854775808ns").is_err());
        assert!(parse_duration("2562048h").is_err());
        assert!(parse_duration("2562047h").is_ok());
        assert_eq!(
            parse_duration("9223372036854775808ns9223372036854775808ns"),
            Ok(())
        );
    }

    #[test]
    fn names_lowercase_as_go_lowercases_them() {
        assert_eq!(go_lowercase("FR\u{0130}"), "fri");
        accepts("0 0 * * FR\u{0130}");
    }

    #[test]
    fn the_error_names_the_schedule() {
        let error = validate_schedule("0 0 * * 7").unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid cron schedule \"0 0 * * 7\": end of range (7) above maximum (6): 7"
        );
    }
}
