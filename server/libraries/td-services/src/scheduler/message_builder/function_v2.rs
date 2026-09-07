//
// Copyright 2026 Tabsdata Inc.
//

use super::info::build_function_info;
use super::inputs::{DispatchPrefetch, resolve_input_tables};
use super::outputs::build_output_tables;
use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::physical_function_run::PhysicalFunctionRunToExecute;
use td_objects::dxo::yaml::function::FunctionInput;
use td_storage::Storage;

/// Assemble the resolved `Function` worker input for one FR: identity
/// info + input tables resolved to physical versions/locations + output
/// tables. The single assembler shared by the supervisor request layer
/// (at = dispatch tick) and the kubernetes resolve endpoint (at = now()),
/// so the two paths can't drift.
pub(crate) async fn build_function_v2<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    storage: &Storage,
    f: &PhysicalFunctionRunToExecute,
    prefetch: &DispatchPrefetch,
) -> Result<FunctionInput, TdError> {
    // All base paths the info block needs are carried on `f` via the to_execute join,
    // so no per-run collection/bundle fetches here.
    let info = build_function_info(storage, f)?;
    let (system_input, input) = resolve_input_tables(conn, storage, f, prefetch).await?;
    let (system_output, output) =
        build_output_tables(storage, f, prefetch.output_tables_for(&f.base.base.base.id))?;

    let function_input_v2 = FunctionInput {
        info,
        system_input,
        input,
        system_output,
        output,
    };
    Ok(function_input_v2)
}
