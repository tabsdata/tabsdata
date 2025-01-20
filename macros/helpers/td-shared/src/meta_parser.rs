//
//  Copyright 2024 Tabs Data Inc.
//

/// A macro to parse meta attributes using the `darling` crate.
///
/// This macro takes a struct that implements `darling::FromMeta` and a `TokenStream`
/// of attribute arguments, and returns an instance of the struct populated with the
/// parsed attribute values. If parsing fails, it returns a `TokenStream` containing
/// the error messages.
///
/// # Arguments
///
/// * `$meta_struct` - The struct type that implements `darling::FromMeta`.
/// * `$args` - The `TokenStream` of attribute arguments to be parsed.
#[macro_export]
macro_rules! parse_meta {
    ($meta_struct:ident, $args:expr) => {
        || -> Result<$meta_struct, proc_macro::TokenStream> {
            let attr_args = match darling::ast::NestedMeta::parse_meta_list($args.into()) {
                Ok(v) => Ok(v),
                Err(e) => Err(proc_macro::TokenStream::from(
                    darling::Error::from(e).write_errors(),
                )),
            }?;

            let args = match $meta_struct::from_list(&attr_args) {
                Ok(v) => Ok(v),
                Err(e) => Err(proc_macro::TokenStream::from(e.write_errors())),
            }?;

            Ok(args)
        }()
    };
}
