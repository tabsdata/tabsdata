//
// Copyright 2025 Tabsdata Inc.
//

use polars::prelude::*;

pub fn drop_prefixed_columns(lf: LazyFrame, prefixes: &[&str]) -> Result<LazyFrame, PolarsError> {
    if prefixes.is_empty() {
        return Ok(lf);
    }
    let schema = lf.clone().collect_schema()?;
    let columns: Vec<Expr> = schema
        .iter_fields()
        .filter_map(|field| {
            let name = field.name();
            if prefixes
                .iter()
                .any(|prefix| name.as_str().starts_with(prefix))
            {
                None
            } else {
                Some(col(name.as_str()))
            }
        })
        .collect();
    Ok(lf.select(columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{TD_COLUMN_PREFIX, TD_CONNECTOR_COLUMN_PREFIX};

    fn frame() -> LazyFrame {
        df!(
            "id" => [0],
            "@td.origin.file.path" => ["f"],
            "$td.id" => [0],
        )
        .unwrap()
        .lazy()
    }

    fn columns(prefixes: &[&str]) -> Vec<String> {
        drop_prefixed_columns(frame(), prefixes)
            .unwrap()
            .collect()
            .unwrap()
            .get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    #[test]
    fn test_drop_prefixed_columns() {
        assert_eq!(columns(&[]), ["id", "@td.origin.file.path", "$td.id"]);
        assert_eq!(columns(&[TD_COLUMN_PREFIX]), ["id", "@td.origin.file.path"]);
        assert_eq!(columns(&[TD_CONNECTOR_COLUMN_PREFIX]), ["id", "$td.id"]);
        assert_eq!(
            columns(&[TD_CONNECTOR_COLUMN_PREFIX, TD_COLUMN_PREFIX]),
            ["id"]
        );
    }
}
