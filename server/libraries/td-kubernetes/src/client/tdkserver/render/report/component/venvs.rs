//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::constants::VENVS;
use crate::client::tdkserver::render::report::helper::table::{
    ReportColumn, ReportTable, render_tables,
};
use crate::core::system::exit::CommandResult;

fn venv_columns() -> [ReportColumn<'static>; 2] {
    [
        ReportColumn {
            header: "Name",
            color_scheme: None,
        },
        ReportColumn {
            header: "Description",
            color_scheme: None,
        },
    ]
}

pub fn venv_list() -> CommandResult {
    let columns = venv_columns();
    render_tables(&[ReportTable {
        title: "Virtual environments",
        columns: &columns,
        rows: VENVS
            .iter()
            .map(|venv| vec![venv.as_str().to_owned(), venv.description().to_owned()])
            .collect(),
    }])
}
