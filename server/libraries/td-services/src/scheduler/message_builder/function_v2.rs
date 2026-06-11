//
// Copyright 2026 Tabs Data Inc.
//

use super::info::build_function_info;
use super::inputs::{DispatchPrefetch, resolve_input_tables};
use super::outputs::build_output_tables;
use td_database::dynamic::DynamicConnection;
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::yaml::function_runner::v2::FunctionInput;
use td_storage::Storage;

/// Assemble the resolved `FunctionV2` worker input for one FR: identity
/// info + input tables resolved to physical versions/locations + output
/// tables. The single assembler shared by the supervisor request layer
/// (at = dispatch tick) and the kubernetes resolve endpoint (at = now()),
/// so the two paths can't drift.
pub(crate) async fn build_function_v2(
    conn: &mut DynamicConnection<'_>,
    storage: &Storage,
    f: &PhysicalFunctionRunToExecute,
    prefetch: &DispatchPrefetch,
) -> Result<FunctionInput, TdError> {
    // All base paths the info block needs are carried on `f` via the to_execute join,
    // so no per-run collection/bundle fetches here.
    let info = build_function_info(storage, f)?;
    let (system_input, input) = resolve_input_tables(conn, storage, f, prefetch).await?;
    let (system_output, output) =
        build_output_tables(storage, f, prefetch.output_tables_for(&f.id))?;

    let function_input_v2 = FunctionInput::builder()
        .info(info)
        .system_input(system_input)
        .input(input)
        .system_output(system_output)
        .output(output)
        .build()?;
    Ok(function_input_v2)
}
