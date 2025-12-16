//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;
use crate::td_pyo3::pyconstant_impl;
use proc_macro::TokenStream;

mod td_pyo3;

/// Attribute macro that exports a const as a Python module variable.
#[proc_macro_attribute]
pub fn pyconstant(args: TokenStream, item: TokenStream) -> TokenStream {
    pyconstant_impl(args, item)
}
