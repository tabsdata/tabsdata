//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use darling::FromMeta;
use darling::util::Flag;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{FnArg, ItemFn, Signature, Type, TypePath, parse_macro_input};
use td_shared::meta_parser::SynMetaOrLit;
use td_shared::parse_meta;

/// Top-level attribute arguments. The two test flavors (`db`, `requires`) are
/// modeled as siblings at the top level rather than nested variants — this
/// gives a flatter grammar (`#[td_test::test(db, fixture = Foo, flavor = "multi_thread")]`)
/// at the cost of needing a runtime check that exactly one of them is set.
#[derive(Debug, FromMeta)]
struct TestArgs {
    /// Marker for a db-backed test. Presence-only.
    #[darling(default)]
    db: Flag,
    /// Fixtures applied after schema upgrade. Only valid alongside `db`.
    #[darling(default, multiple, rename = "fixture")]
    fixtures: Vec<FixturePath>,

    /// External-resource requirements provider type. Presence selects the
    /// requires-flavor; the test runs only when its env vars are present.
    #[darling(default)]
    requires: Option<Ident>,
    /// Env-var prefix that scopes the requires lookup. Required when `requires`
    /// is set.
    #[darling(default)]
    env_prefix: Option<SynMetaOrLit>,

    /// Tokio runtime flavor forwarded to the auto-emitted `#[::tokio::test]`.
    #[darling(default)]
    flavor: Option<String>,
    /// `worker_threads = N` forwarded to `#[::tokio::test(...)]`. Only
    /// meaningful with `flavor = "multi_thread"`.
    #[darling(default)]
    worker_threads: Option<u32>,
}

/// One backend arm. For db: one per backend (`sqlite`, `postgres`). Each arm
/// is gated by a cfg-toggle macro in td-test (`::td_test::__cfg_<name>!`),
/// not a `#[cfg(<name>)]` attribute in the consumer crate — that way
/// downstream callers don't need a build.rs trampoline to declare the
/// backend cfgs.
struct DbArm {
    backend_name: &'static str,
    setup_expr: TokenStream2,
}

pub fn scoped_test(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_meta!(TestArgs, args).unwrap();
    let input = parse_macro_input!(item as ItemFn);

    let func_vis = &input.vis;
    let original_sig = &input.sig;
    let func_body = &input.block;
    let func_attrs = &input.attrs;

    let ret_ty = &original_sig.output;
    let returns_result = !matches!(ret_ty, syn::ReturnType::Default);

    let tokio_attr = match (args.flavor.as_deref(), args.worker_threads) {
        (None, None) => quote! { #[::tokio::test] },
        (Some(flavor), None) => quote! { #[::tokio::test(flavor = #flavor)] },
        (Some(flavor), Some(n)) => {
            quote! { #[::tokio::test(flavor = #flavor, worker_threads = #n)] }
        }
        (None, Some(n)) => quote! { #[::tokio::test(worker_threads = #n)] },
    };

    let original_name = &original_sig.ident;

    let expanded = match (args.db.is_present(), &args.requires) {
        (true, None) => {
            let (fn_args, arms) = db_arms(original_sig, &args.fixtures);
            emit_db_wrapper(
                original_name,
                func_vis,
                func_attrs,
                func_body,
                &fn_args,
                ret_ty,
                returns_result,
                &tokio_attr,
                &arms,
            )
        }
        (false, Some(reqs)) => {
            let env_prefix = args
                .env_prefix
                .as_ref()
                .expect("`env_prefix = \"...\"` is required when using `requires = ...`");
            if !args.fixtures.is_empty() {
                panic!("`fixture = ...` is only valid with the `db` flavor");
            }
            let (fn_args, setup_expr) =
                requires_arm(original_sig, reqs, env_prefix);
            emit_requires(
                original_name,
                func_vis,
                func_attrs,
                func_body,
                &fn_args,
                ret_ty,
                returns_result,
                &tokio_attr,
                &setup_expr,
            )
        }
        (true, Some(_)) => panic!("`db` and `requires = ...` are mutually exclusive — pick one"),
        (false, None) => panic!("must specify either `db` or `requires = <Type>`"),
    };

    expanded.into()
}

/// Emit one `#[::tokio::test]` per backend. The **first** backend (sqlite)
/// keeps the original test name so `cargo test --exact <name>` from an IDE
/// gutter click finds it; subsequent backends are suffixed (e.g. `_postgres`).
/// Each arm is `cfg`-gated, so the unsuffixed arm only exists when the first
/// backend in the matrix is compiled in.
///
/// The user's body is hoisted into a shared `__td_test_inner_<name>` helper at
/// the parent module level so per-backend arms don't duplicate the body and
/// the helper retains access to private items in the same `mod tests`.
#[allow(clippy::too_many_arguments)]
fn emit_db_wrapper(
    original_name: &Ident,
    func_vis: &syn::Visibility,
    func_attrs: &[syn::Attribute],
    func_body: &syn::Block,
    fn_args: &[(Ident, TypePath)],
    ret_ty: &syn::ReturnType,
    returns_result: bool,
    tokio_attr: &TokenStream2,
    arms: &[DbArm],
) -> TokenStream2 {
    let (arg_names, arg_types): (Vec<_>, Vec<_>) =
        fn_args.iter().map(|(n, t)| (n, t)).unzip();

    let (ret_sig, ret_if_skip) = if returns_result {
        (quote! { #ret_ty }, quote! { Ok(()) })
    } else {
        (quote! { -> () }, quote! {})
    };

    let inner_name = format_ident!("__td_test_inner_{}", original_name);

    let test_fns = arms.iter().enumerate().map(|(i, arm)| {
        // First backend keeps the original ident so IDE
        // `cargo test --exact <name>` matches it. Later backends are suffixed.
        let test_name = if i == 0 {
            original_name.clone()
        } else {
            format_ident!("{}_{}", original_name, arm.backend_name)
        };
        let cfg_macro = format_ident!("__cfg_{}", arm.backend_name);
        let setup = &arm.setup_expr;
        // Wrap each arm in `::td_test::__cfg_<backend>!{ ... }` rather than
        // prefixing with `#[cfg(<backend>)]`. The macro is defined in td-test
        // (where backend cfgs are declared via the build.rs trampoline) and
        // expands to either its body or nothing depending on which version
        // got compiled in — moving the cfg lookup out of the consumer crate.
        quote! {
            ::td_test::#cfg_macro! {
                #(#func_attrs)*
                #tokio_attr
                #func_vis async fn #test_name() #ret_sig {
                    use ::td_test::__private::TestSetup;
                    match #setup {
                        ::td_test::__private::TestSetupExecution::Skip => {
                            eprintln!("[td-test] SKIP: {}", stringify!(#test_name));
                            #ret_if_skip
                        }
                        ::td_test::__private::TestSetupExecution::Run(t) => {
                            #inner_name(t).await
                        }
                    }
                }
            }
        }
    });

    quote! {
        #(#func_attrs)*
        #[allow(dead_code)]
        async fn #inner_name(#(#arg_names: #arg_types),*) #ret_ty
            #func_body

        #(#test_fns)*
    }
}

/// Emit the requires (single-arm) form: one test fn at module level with the
/// original name. No matrix.
#[allow(clippy::too_many_arguments)]
fn emit_requires(
    original_name: &Ident,
    func_vis: &syn::Visibility,
    func_attrs: &[syn::Attribute],
    func_body: &syn::Block,
    fn_args: &[(Ident, TypePath)],
    ret_ty: &syn::ReturnType,
    returns_result: bool,
    tokio_attr: &TokenStream2,
    setup_expr: &TokenStream2,
) -> TokenStream2 {
    let (arg_names, arg_types): (Vec<_>, Vec<_>) =
        fn_args.iter().map(|(n, t)| (n, t)).unzip();

    let (ret_sig, ret_if_skip) = if returns_result {
        (quote! { #ret_ty }, quote! { Ok(()) })
    } else {
        (quote! { -> () }, quote! {})
    };

    quote! {
        #(#func_attrs)*
        #tokio_attr
        #func_vis async fn #original_name() #ret_sig {
            use ::td_test::__private::TestSetup;

            async fn inner(#(#arg_names: #arg_types),*) #ret_sig
                #func_body

            match #setup_expr {
                ::td_test::__private::TestSetupExecution::Skip => {
                    eprintln!("[td-test] SKIP: {}", stringify!(#original_name));
                    #ret_if_skip
                },
                ::td_test::__private::TestSetupExecution::Run(t) => {
                    inner(t).await
                }
            }
        }
    }
}

/// A wrapper around `syn::Path` that implements `FromMeta` for darling parsing.
///
/// Allows `fixture = some::path::Type` in proc macro attributes.
#[derive(Debug, Clone)]
struct FixturePath(syn::Path);

impl FromMeta for FixturePath {
    fn from_expr(expr: &syn::Expr) -> darling::Result<Self> {
        match expr {
            syn::Expr::Path(expr_path) => Ok(FixturePath(expr_path.path.clone())),
            _ => Err(darling::Error::unexpected_expr_type(expr)),
        }
    }
}

/// The set of backends the macro emits arms for. Each arm is `cfg`-gated; if
/// the cfg is off at the call site, the arm is stripped before type-checking.
const BACKENDS: &[(&str, &str)] = &[
    // (provider type path suffix, NAME constant string)
    // The first backend keeps the original test fn name (matches IDE
    // `--exact <ident>` gutter clicks); subsequent backends get `_<name>`
    // suffixed names. Postgres is first because it's the default backend
    // (see `DEFAULT_BACKENDS` in `td-test/src/db/mod.rs`) — keeping the IDE
    // click in sync with what `cargo test` actually runs by default.
    ("postgres::PostgresProvider", "postgres"),
    ("sqlite::SqliteProvider", "sqlite"),
];

fn db_arms(
    func_sig: &Signature,
    fixtures: &[FixturePath],
) -> (Vec<(Ident, TypePath)>, Vec<DbArm>) {
    let fixture_builders = fixtures
        .iter()
        .map(|f| {
            let path = &f.0;
            quote! { <#path as ::td_schema::schema::Fixture>::build }
        })
        .collect::<Vec<_>>();
    // No fixtures → install the full schema; with fixtures, the fixture itself
    // is responsible for setting up its own tables.
    let install_schema = fixture_builders.is_empty();

    let (name, ty) = get_fn_args(func_sig, &Ident::new("TdPool", Span::call_site()));

    let arms = BACKENDS
        .iter()
        .map(|(provider_path, backend_name)| {
            let provider_segments: TokenStream2 =
                syn::parse_str(provider_path).expect("invalid provider path");
            let fixture_builders = fixture_builders.clone();
            DbArm {
                backend_name,
                setup_expr: quote! {
                    ::td_test::db::DbTestSetup::<::td_test::db::#provider_segments>::new(
                        #install_schema,
                        vec![#(#fixture_builders),*],
                    ).setup().await
                },
            }
        })
        .collect();

    (vec![(name, ty)], arms)
}

fn requires_arm(
    func_sig: &Signature,
    reqs: &Ident,
    env_prefix: &SynMetaOrLit,
) -> (Vec<(Ident, TypePath)>, TokenStream2) {
    let test_name = &func_sig.ident.to_string();
    let (name, ty) = get_fn_args(func_sig, reqs);

    let setup_expr = quote! {
        ::td_test::reqs::ReqsTestSetup::<#reqs>::new(#test_name, #env_prefix).setup().await
    };

    (vec![(name, ty)], setup_expr)
}

fn get_fn_args(func_sig: &Signature, kind: &Ident) -> (Ident, TypePath) {
    func_sig
        .inputs
        .iter()
        .find_map(|arg| {
            if let FnArg::Typed(pat_type) = arg
                && let Type::Path(type_path) = &*pat_type.ty
                && type_path
                    .path
                    .segments
                    .iter()
                    .any(|segment| segment.ident == *kind)
                && let syn::Pat::Ident(pat_ident) = &*pat_type.pat
            {
                return Some((pat_ident.ident.clone(), type_path.clone()));
            }
            None
        })
        .unwrap_or_else(|| panic!("Function must have a {kind} parameter"))
}
