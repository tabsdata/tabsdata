//
// Copyright 2026 Tabsdata Inc.
//

//! `requires`-flavor emission: the test runs only when the provider's env vars
//! are present. The wrapper itself is
//! [`emit_injected_arg`](crate::common::emit_injected_arg) — this arm only
//! builds the setup.

use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::quote;

use td_shared::meta_parser::SynMetaOrLit;

pub(crate) fn requires_setup_expr(
    test_name: &str,
    reqs: &Ident,
    env_prefix: &SynMetaOrLit,
) -> TokenStream2 {
    quote! {
        ::td_test::reqs::ReqsTestSetup::<#reqs>::new(#test_name, #env_prefix).setup().await
    }
}
