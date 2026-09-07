//
// Copyright 2026 Tabsdata Inc.
//

use polars::prelude::{LazyFrame, PolarsError};
use td_objects::dxo::table::ExtraCols;
use td_tableframe::common::drop_prefixed_columns;
use td_tableframe::constants::{TD_COLUMN_PREFIX, TD_CONNECTOR_COLUMN_PREFIX};

pub fn drop_extra_columns(
    lazy_frame: LazyFrame,
    extra_cols: &ExtraCols,
) -> Result<LazyFrame, PolarsError> {
    let dropped: &[&str] = match extra_cols {
        ExtraCols::None => &[TD_CONNECTOR_COLUMN_PREFIX, TD_COLUMN_PREFIX],
        ExtraCols::Meta => &[TD_COLUMN_PREFIX],
        ExtraCols::Sys => &[TD_CONNECTOR_COLUMN_PREFIX],
        ExtraCols::All => &[],
    };
    drop_prefixed_columns(lazy_frame, dropped)
}
