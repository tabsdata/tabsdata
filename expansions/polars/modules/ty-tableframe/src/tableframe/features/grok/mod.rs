//
// Copyright 2025 Tabsdata Inc.
//

pub mod engine;

use engine::functions as grok_fns;

use dashmap::DashMap;
use grok::Grok;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::{PyResult, pyfunction};
use pyo3_polars::derive::polars_expr;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use serde::Deserialize;
use std::collections::BTreeMap;
use td_common::logging;
use td_common::logging::LoggingOptions;
use tracing::{Level, debug};

struct GrokContext {
    matcher: Arc<grok::Pattern>,
    captures: Arc<[String]>,
}

const GROK_CONTEXT_CACHE_CAPACITY: usize = 128;

static GROK_CONTEXT_CACHE: Lazy<DashMap<String, Arc<GrokContext>>> = Lazy::new(DashMap::new);

fn get_matcher_and_captures(pattern: &str) -> PolarsResult<Arc<GrokContext>> {
    if let Some(grok_context) = GROK_CONTEXT_CACHE.get(pattern) {
        debug!("Hitting grok context cache for pattern: {}", pattern);
        return Ok(Arc::clone(&*grok_context));
    }
    let grok = Grok::default();
    let matcher = Arc::new(grok_fns::grok_compile(&grok, pattern).map_err(
        |error| polars_err!(ComputeError: "Grok compile error for '{}': {}", pattern, error),
    )?);
    let captures: Arc<[String]> = grok_fns::grok_fields(&matcher)
        .map_err(|error| polars_err!(ComputeError: "Failed to list captures of '{}': {}", pattern, error))?
        .into();
    if captures.is_empty() {
        return Err(
            polars_err!(ComputeError: "No capture groups found in grok pattern '{}'", pattern),
        );
    }
    let grok_context = Arc::new(GrokContext { matcher, captures });
    if GROK_CONTEXT_CACHE.len() >= GROK_CONTEXT_CACHE_CAPACITY {
        GROK_CONTEXT_CACHE.clear();
    }
    if let Some(grok_context_hit) =
        GROK_CONTEXT_CACHE.insert(pattern.to_string(), Arc::clone(&grok_context))
    {
        return Ok(grok_context_hit);
    }
    Ok(grok_context)
}

/// Arguments passed to the `_grok` Polars expression.
///
/// # Fields
/// - `temp_column`: Name of the (temporary) output column that will hold the
///   struct with the extracted captures.
/// - `_index`: index of the underlying TableFrame; currently unused but reserved
///   for future use.
/// - `pattern`: Grok pattern to apply to each input value.
/// - `mapping`: Ordered mapping from *capture name* → *output column name*.
///   The **keys** must match capture names present in `pattern`, and the
///   **values** name the fields of the resulting struct, in the order of the
///   keys. Only keys in mapping are returned back as fields.
///
#[derive(Deserialize, Debug)]
pub struct GrokKwargs {
    temp_column: String,
    _index: Option<i64>,
    pattern: String,
    mapping: IndexMap<String, String>,
}

/*
Functions marked as 'polars_expr' cannot be easily tested. To allow having test, we use the pattern
function (polars expr) + function-impl (actual implementation).
 */

fn grok_fields_out(kwargs: &GrokKwargs) -> PolarsResult<Vec<Field>> {
    if kwargs.mapping.is_empty() {
        return Err(polars_err!(ComputeError: "Mapping cannot be empty"));
    }
    let grok_context = get_matcher_and_captures(&kwargs.pattern)?;
    let mut names = PlHashSet::with_capacity(kwargs.mapping.len());
    kwargs
        .mapping
        .iter()
        .map(|(capture, name)| {
            if !grok_context.captures.contains(capture) {
                return Err(
                    polars_err!(ComputeError: "Mapping key '{}' not found in grok captures of '{}'", capture, kwargs.pattern),
                );
            }
            if !names.insert(name.as_str()) {
                return Err(
                    polars_err!(Duplicate: "Column '{}' is produced by more than one grok capture", name),
                );
            }
            Ok(Field::new(name.as_str().into(), DataType::String))
        })
        .collect()
}

fn grok_schema(_: &[Field], kwargs: GrokKwargs) -> PolarsResult<Field> {
    Ok(Field::new(
        kwargs.temp_column.as_str().into(),
        DataType::Struct(grok_fields_out(&kwargs)?),
    ))
}

#[inline]
#[polars_expr(output_type_func_with_kwargs = grok_schema)]
/// Apply a Grok pattern to a single `pl.String` input column and return per-row
/// captures.
///
/// # Behavior
/// - Expects exactly **one** input `Series` in `batch`; returns an error otherwise.
/// - Return a `Series` named `temp_column` with dtype `Struct` of `pl.String`
///   fields named by the `mapping` values, and one element per input row. A null
///   or unmatched input row leaves every field null.
///
/// # Errors
/// - If `batch.len() != 1`.
/// - If the input series is not String.
/// - If the pattern has no captures.
/// - If any `mapping` key is not found among the pattern captures, or two keys
///   map to the same column (raised when the plan resolves its schema).
///
/// # Notes
/// - The field order is deterministic and matches the insertion order of
///   `mapping` (hence the use of `IndexMap`).
/// - Polars names the output after the input column, so the Python side aliases
///   it to `temp_column` and unnests it to get one column per capture.
///
pub fn _grok(batch: &[Series], kwargs: GrokKwargs) -> PolarsResult<Series> {
    logging::start_with(LoggingOptions {
        max_level: Some(Level::ERROR),
        ..Default::default()
    });
    debug!(
        "Executing grok expression function with parameters: {:?}",
        kwargs
    );
    _grok_impl(batch, kwargs)
}

/*
Visit this tutorial if you gave up trying to understand what is going on here:
https://marcogorelli.github.io/polars-plugins-tutorial/struct/
 */
#[inline]
fn _grok_impl(batch: &[Series], kwargs: GrokKwargs) -> PolarsResult<Series> {
    if batch.len() != 1 {
        return Err(
            polars_err!(ComputeError: "Expected exactly one input series; got {}", batch.len()),
        );
    }

    let series_in = &batch[0];
    let q_rows_in = series_in.len();

    let grok_context = get_matcher_and_captures(&kwargs.pattern)?;

    let chunked_array_in = series_in
        .str()
        .map_err(|_| polars_err!(ComputeError: "Input series must be of type pl.String"))?;

    let mut data_out: Vec<StringChunkedBuilder> = kwargs
        .mapping
        .values()
        .map(|name| StringChunkedBuilder::new(name.as_str().into(), q_rows_in))
        .collect();

    for row_in in chunked_array_in.iter() {
        let matched = row_in.and_then(|input| grok_context.matcher.match_against(input));
        match matched {
            Some(matches) => {
                for (capture, builder) in kwargs.mapping.keys().zip(data_out.iter_mut()) {
                    builder.append_option(matches.get(capture));
                }
            }
            None => data_out
                .iter_mut()
                .for_each(|builder| builder.append_null()),
        }
    }

    let fields_out: Vec<Series> = data_out
        .into_iter()
        .map(|builder| builder.finish().into_series())
        .collect();
    Ok(StructChunked::from_series(
        kwargs.temp_column.as_str().into(),
        q_rows_in,
        fields_out.iter(),
    )?
    .into_series())
}

#[inline]
#[gen_stub_pyfunction(module = "tabsdata.expansions.tableframe.features.grok.engine")]
#[pyfunction]
pub fn grok_patterns() -> PyResult<BTreeMap<&'static str, &'static str>> {
    grok_fns::grok_patterns()
}

#[inline]
#[gen_stub_pyfunction(module = "tabsdata.expansions.tableframe.features.grok.engine")]
#[pyfunction]
pub fn grok_fields(pattern: &str) -> PyResult<Vec<String>> {
    let grok = Grok::default();
    let matcher = grok.compile(pattern, false).map_err(|error| {
        PyValueError::new_err(format!(
            "Failed to obtain a matcher from the Grok pattern '{}': {}",
            pattern, error
        ))
    })?;
    grok_fns::grok_fields(&matcher)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kwargs(pattern: &str, mapping: &[(&str, &str)]) -> GrokKwargs {
        GrokKwargs {
            temp_column: "parsed".to_string(),
            _index: Some(-1),
            pattern: pattern.to_string(),
            mapping: mapping
                .iter()
                .map(|(capture, name)| (capture.to_string(), name.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_grok_schema_is_a_struct_named_after_the_temp_column() {
        let field = grok_schema(
            &[],
            kwargs(
                r"%{WORD:verb} %{INT:count}",
                &[("count", "times"), ("verb", "verb")],
            ),
        )
        .unwrap();
        assert_eq!(field.name().as_str(), "parsed");
        assert_eq!(
            field.dtype(),
            &DataType::Struct(vec![
                Field::new("times".into(), DataType::String),
                Field::new("verb".into(), DataType::String),
            ])
        );
    }

    #[test]
    fn test_grok_schema_refuses_a_bad_mapping() {
        let pattern = r"%{WORD:verb} %{INT:count}";
        assert!(grok_schema(&[], kwargs(pattern, &[])).is_err());
        assert!(grok_schema(&[], kwargs(pattern, &[("missing", "missing")])).is_err());
        assert!(grok_schema(&[], kwargs(pattern, &[("verb", "x"), ("count", "x")])).is_err());
    }

    #[test]
    fn test_grok_extracts_one_field_per_capture() {
        let input = Series::new("line".into(), [Some("load 7"), None, Some("nothing")]);
        let make = || {
            kwargs(
                r"%{WORD:verb} %{INT:count}",
                &[("count", "times"), ("verb", "verb")],
            )
        };
        let expected = grok_schema(&[], make()).unwrap();

        let output = _grok_impl(&[input], make()).unwrap();

        assert_eq!(output.name(), expected.name());
        assert_eq!(output.dtype(), expected.dtype());
        let fields = output.struct_().unwrap().fields_as_series();
        let times: Vec<Option<&str>> = fields[0].str().unwrap().iter().collect();
        let verb: Vec<Option<&str>> = fields[1].str().unwrap().iter().collect();
        assert_eq!(times, [Some("7"), None, None]);
        assert_eq!(verb, [Some("load"), None, None]);
    }
}
