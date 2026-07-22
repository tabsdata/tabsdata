//
// Copyright 2025 Tabsdata Inc.
//

pub const TD_COLUMN_PREFIX: &str = "$td.";
pub const TD_COLUMN_PREFIX_REGEXP: &str = r"^\$td\..*";

pub const TD_CONNECTOR_COLUMN_PREFIX: &str = "@td.";

pub const TD_ATTR_ORIGIN: &str = "origin";
pub const TD_ATTR_FILE: &str = "file";
pub const TD_ATTR_PATH: &str = "path";
pub const TD_ATTR_LINE: &str = "line";

pub const TD_ATTR_OPT: &str = "opt";
pub const TD_ATTR_TST: &str = "tst";
pub const TD_ATTR_SRV: &str = "srv";
pub const TD_ATTR_USR: &str = "usr";
pub const TD_ATTR_TS: &str = "ts";
pub const TD_ATTR_FN: &str = "fn";

macro_rules! td_prefix {
    ($($segment:expr),+) => {
        concat!("$td." $(, $segment, ".")+)
    };
}

macro_rules! td_column_name {
    ($first:expr $(, $rest:expr)*) => {
        concat!("$td.", $first $(, ".", $rest)*)
    };
}

macro_rules! td_connector_prefix {
    ($($segment:expr),+) => {
        concat!("@td." $(, $segment, ".")+)
    };
}

macro_rules! td_connector_column_name {
    ($first:expr $(, $rest:expr)*) => {
        concat!("@td.", $first $(, ".", $rest)*)
    };
}

pub const TD_ORIGIN_COLUMN_PREFIX: &str = td_connector_prefix!("origin");
pub const TD_ORIGIN_FILE_PREFIX: &str = td_connector_prefix!("origin", "file");
pub const TD_ORIGIN_FILE_PATH: &str = td_connector_column_name!("origin", "file", "path");
pub const TD_ORIGIN_FILE_LINE: &str = td_connector_column_name!("origin", "file", "line");

pub const TD_OPT_COLUMN_PREFIX: &str = td_prefix!("opt");
pub const TD_OPT_TST_PREFIX: &str = td_prefix!("opt", "tst");
pub const TD_OPT_TST_SRV: &str = td_column_name!("opt", "tst", "srv");
pub const TD_OPT_TST_USR: &str = td_column_name!("opt", "tst", "usr");
pub const TD_OPT_TST_TS: &str = td_column_name!("opt", "tst", "ts");
pub const TD_OPT_TST_FN: &str = td_column_name!("opt", "tst", "fn");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_column_values() {
        assert_eq!(TD_ORIGIN_COLUMN_PREFIX, "@td.origin.");
        assert_eq!(TD_ORIGIN_FILE_PREFIX, "@td.origin.file.");
        assert_eq!(TD_ORIGIN_FILE_PATH, "@td.origin.file.path");
        assert_eq!(TD_ORIGIN_FILE_LINE, "@td.origin.file.line");
    }

    #[test]
    fn test_opt_column_values() {
        assert_eq!(TD_OPT_COLUMN_PREFIX, "$td.opt.");
        assert_eq!(TD_OPT_TST_PREFIX, "$td.opt.tst.");
        assert_eq!(TD_OPT_TST_SRV, "$td.opt.tst.srv");
        assert_eq!(TD_OPT_TST_USR, "$td.opt.tst.usr");
        assert_eq!(TD_OPT_TST_TS, "$td.opt.tst.ts");
        assert_eq!(TD_OPT_TST_FN, "$td.opt.tst.fn");
    }
}
