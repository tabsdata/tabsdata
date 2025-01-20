//
//   Copyright 2024 Tabs Data Inc.
//

use std::ops::Deref;
use td_common::error::TdError;
use td_execution::parameters::{
    FunctionInput, FunctionInputV1, Info, InputTable, OutputTable, TablePosition,
};
use td_tower::extractors::Input;

pub async fn build_function_input_v1(
    Input(info): Input<Info>,
    Input(input): Input<Vec<InputTable>>,
    Input(output): Input<Vec<OutputTable>>,
) -> Result<FunctionInput, TdError> {
    let (system_input, input) = split_tables(input.deref().clone());
    let (system_output, output) = split_tables(output.deref().clone());

    let function_input_v1 = FunctionInputV1::builder()
        .info(info.deref().clone())
        .system_input(system_input)
        .input(input)
        .system_output(system_output)
        .output(output)
        .build()
        .unwrap();
    let function_input = FunctionInput::V1(Box::new(function_input_v1));
    Ok(function_input)
}

fn split_tables<T: TablePosition>(tables: Vec<T>) -> (Vec<T>, Vec<T>) {
    // Tables with positions < 0 are system tables.
    let (system_tables, user_tables): (Vec<T>, Vec<T>) =
        tables.into_iter().partition(|table| table.position() < 0);
    (system_tables, user_tables)
}
