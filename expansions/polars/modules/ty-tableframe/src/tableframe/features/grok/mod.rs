//
// Copyright 2025 Tabs Data Inc.
//

pub mod engine;

use engine::functions as grok_fns;
use engine::functions::grok_schema;

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
use td_common::logging::LogOutput;
use tracing::{Level, debug};

struct GrokContext {
    matcher: Arc<grok::Pattern>,
    captures: Arc<[String]>,
}

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
///   list/struct with the extracted captures.
/// - `_index`: index of the underlying TableFrame; currently unused but reserved
///   for future use.
/// - `pattern`: Grok pattern to apply to each input value.
/// - `mapping`: Ordered mapping from *capture name* → *output column name*.
///   The **keys** must match capture names present in `pattern`. The **order
///   of keys** controls the order of values in the resulting list per row (and
///   therefore the order if later converted to a struct). Only keys in mapping
///   are returned back as values.
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

#[inline]
#[polars_expr(output_type_func = grok_schema)]
/// Apply a Grok pattern to a single `pl.String` input column and return per-row
/// captures.
///
/// # Behavior
/// - Expects exactly **one** input `Series` in `batch`; returns an error otherwise.
/// - Return a `Series` named `temp_column` with dtype `List<pl.String>` and one
///   element per input row.
///
/// # Errors
/// - If `batch.len() != 1`.
/// - If the input series is not String.
/// - If the pattern has no captures.
/// - If any `mapping` key is not found among the pattern captures.
///
/// # Notes
/// - The returned list order is deterministic and matches the insertion order of
///   `mapping` (hence the use of `IndexMap`).
/// - You can convert the list to a struct with named fields on the Python side:
///   `pl.col("__grok_tmp").list.to_struct(fields=[...]).struct.unnest()`.
///
pub fn _grok(batch: &[Series], kwargs: GrokKwargs) -> PolarsResult<Series> {
    logging::start(Level::ERROR, Some(LogOutput::StdOut), false);
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
    let captures_out = &grok_context.captures;

    let fields_out: Vec<String> = kwargs.mapping.keys().cloned().collect();
    if fields_out.is_empty() {
        return Err(polars_err!(ComputeError: "Mapping cannot be empty"));
    }
    for field in &fields_out {
        if !captures_out.contains(field) {
            return Err(
                polars_err!(ComputeError: "Mapping key '{}' not found in grok captures of '{}'", field, kwargs.pattern),
            );
        }
    }

    let chunked_array_in = series_in
        .str()
        .map_err(|_| polars_err!(ComputeError: "Input series must be of type pl.String"))?;

    let mut data_out = ListStringChunkedBuilder::new(
        kwargs.temp_column.as_str().into(),
        q_rows_in,
        q_rows_in * fields_out.len(),
    );

    for row_in in chunked_array_in.into_iter() {
        match row_in {
            Some(input) => {
                let values_out: Vec<Option<String>> =
                    grok_fns::grok_values(&grok_context.matcher, input, &fields_out);
                let row_out = Series::new(PlSmallStr::from(""), values_out);
                data_out.append_series(&row_out)?;
            }
            None => data_out.append_null(),
        }
    }
    Ok(data_out.finish().into_series())
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
