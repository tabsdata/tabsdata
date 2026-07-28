//
// Copyright 2026 Tabsdata Inc.
//

use td_error::TdError;

/// Source as the client registered it. Only ever an inbound value: the
/// server trims it down to a [`Snippet`] before storing it.
#[ta_objects::typed(string(min_len = 0, max_len = RAW_SNIPPET_MAX_LEN))]
#[derive(Eq, PartialEq)]
pub struct RawSnippet;

/// The stored, user-facing source of a function: display metadata, not
/// the code that runs — that lives in the function's bundle.
#[ta_objects::typed(string(min_len = 0, max_len = SNIPPET_MAX_LEN))]
#[derive(Eq, PartialEq)]
pub struct Snippet;

pub const RAW_SNIPPET_MAX_LEN: usize = 1024 * 1024;
pub const SNIPPET_MAX_LEN: usize = 32 * 1024;

/// Bytes of a trim budget held back for the elision marker.
const SNIPPET_MARKER_RESERVE: usize = 64;

/// Trims the registered source down to what the server stores. Snippets
/// are display metadata, so trimming one is preferable to rejecting the
/// registration.
impl TryFrom<&RawSnippet> for Snippet {
    type Error = TdError;

    fn try_from(raw: &RawSnippet) -> Result<Self, Self::Error> {
        Self::try_from(raw.trimmed(SNIPPET_MAX_LEN))
    }
}

impl TryFrom<RawSnippet> for Snippet {
    type Error = TdError;

    fn try_from(raw: RawSnippet) -> Result<Self, Self::Error> {
        Self::try_from(&raw)
    }
}

impl RawSnippet {
    /// Head and tail of the source around an elision marker, cut on line
    /// boundaries and never longer than `budget` bytes. The head gets two
    /// thirds of the budget — the decorator and signature matter most, the
    /// return statement second.
    fn trimmed(&self, budget: usize) -> String {
        let source = self.as_str();
        if source.len() <= budget {
            return source.to_string();
        }
        let room = budget.saturating_sub(SNIPPET_MARKER_RESERVE);
        let head_end = line_end_before(source, room * 2 / 3);
        let tail_start = line_start_after(source, source.len() - (room - head_end));
        let head = &source[..head_end];
        let separator = if head.ends_with('\n') { "" } else { "\n" };
        format!(
            "{head}{separator}# ... {} bytes elided ...\n{}",
            tail_start - head_end,
            &source[tail_start..]
        )
    }
}

fn line_end_before(source: &str, limit: usize) -> usize {
    let limit = floor_boundary(source, limit);
    source[..limit].rfind('\n').map_or(limit, |index| index + 1)
}

fn line_start_after(source: &str, from: usize) -> usize {
    let from = ceil_boundary(source, from);
    source[from..]
        .find('\n')
        .map_or(from, |index| from + index + 1)
}

fn floor_boundary(source: &str, index: usize) -> usize {
    let mut index = index.min(source.len());
    while !source.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_boundary(source: &str, index: usize) -> usize {
    let mut index = index.min(source.len());
    while !source.is_char_boundary(index) {
        index += 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    fn module_source(lines: usize) -> String {
        let mut source = String::from("import tabsdata as td\n\n\n@td.publisher()\ndef f():\n");
        for line in 0..lines {
            source.push_str(&format!("    value_{line} = df.select('col_{line}')\n"));
        }
        source.push_str("    return value_0\n");
        source
    }

    #[test]
    fn test_raw_snippet_that_fits_is_stored_whole() -> Result<(), TdError> {
        let source = module_source(10);
        let raw = RawSnippet::try_from(source.as_str())?;

        assert_eq!(*Snippet::try_from(&raw)?, source);
        Ok(())
    }

    #[test]
    fn test_oversized_raw_snippet_keeps_head_and_tail() -> Result<(), TdError> {
        let source = module_source(4000);
        assert!(source.len() > SNIPPET_MAX_LEN);
        let raw = RawSnippet::try_from(source.as_str())?;

        let stored = Snippet::try_from(&raw)?;

        assert!(stored.len() <= SNIPPET_MAX_LEN);
        assert!(stored.starts_with("import tabsdata as td\n"));
        assert!(stored.ends_with("    return value_0\n"));
        assert!(stored.contains(" bytes elided ..."));
        Ok(())
    }

    #[test]
    fn test_trimmed_cuts_on_line_boundaries() -> Result<(), TdError> {
        let raw = RawSnippet::try_from(module_source(4000).as_str())?;

        let trimmed = raw.trimmed(4096);

        assert!(trimmed.len() <= 4096);
        for line in trimmed.lines() {
            assert!(
                line.is_empty()
                    || line.starts_with("import ")
                    || line.starts_with("@td.")
                    || line.starts_with("def ")
                    || line.starts_with("    ")
                    || line.starts_with("# ... "),
                "partial line in trimmed snippet: {line:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_trimmed_reports_exactly_the_bytes_it_dropped() -> Result<(), TdError> {
        let source = module_source(4000);
        let raw = RawSnippet::try_from(source.as_str())?;

        let trimmed = raw.trimmed(SNIPPET_MAX_LEN);

        let (head, rest) = trimmed.split_once("# ... ").expect("marker");
        let (elided, tail) = rest.split_once(" bytes elided ...\n").expect("marker");
        let head = if source.starts_with(head) {
            head
        } else {
            head.strip_suffix('\n').expect("separator")
        };
        assert!(source.starts_with(head));
        assert!(source.ends_with(tail));
        assert_eq!(
            head.len() + elided.parse::<usize>().unwrap() + tail.len(),
            source.len()
        );
        Ok(())
    }

    #[test]
    fn test_trimmed_holds_the_budget_on_multibyte_sources() -> Result<(), TdError> {
        let raw = RawSnippet::try_from("# ñ\n".repeat(20000).as_str())?;

        for budget in [1024, 2048, SNIPPET_MAX_LEN] {
            let trimmed = raw.trimmed(budget);
            assert!(trimmed.len() <= budget, "budget {budget} exceeded");
            assert!(Snippet::try_from(trimmed).is_ok());
        }
        Ok(())
    }

    #[test]
    fn test_snippet_rejects_over_max_len() {
        assert!(Snippet::try_from("x".repeat(SNIPPET_MAX_LEN)).is_ok());
        assert!(Snippet::try_from("x".repeat(SNIPPET_MAX_LEN + 1)).is_err());
    }
}
