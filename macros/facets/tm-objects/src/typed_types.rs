//
// Copyright 2025 Tabsdata Inc.
//

use darling::FromMeta;
use darling::util::parse_expr::preserve_str_literal;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use syn::{DeriveInput, Expr, Type, parse_macro_input};

// ---------------------------------------------------------------------------
// Newtype facet derives.
//
// `#[derive(Typed)]` is the domain core: it implements the construction gate
// (`ta_objects::Typed::try_new`, from the `#[typed(...)]` constraints) and
// everything derived from it. The external contracts are separate facet
// derives so a declaration's derive list shows its participation:
// `TypedSerde` (wire), `TypedSqlx` (database), `TypedOpenApi` (API docs).
// Facets require `Typed` — their emissions call `<Self as
// ta_objects::Typed>::try_new`, so every construction path routes through
// the gate and a held value always satisfies its constraints.
// ---------------------------------------------------------------------------

pub fn derive_typed(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if let Some(t) = crate::brand::brand_param(&input) {
        return match reject_brand_args(&input) {
            Ok(()) => crate::brand::expand_typed(&input.ident, &t).into(),
            Err(e) => e.to_compile_error().into(),
        };
    }
    match expand_typed(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Brands take no `#[typed(...)]` constraints — they are transparent.
fn reject_brand_args(input: &DeriveInput) -> syn::Result<()> {
    match input.attrs.iter().find(|a| a.path().is_ident("typed")) {
        Some(attr) => Err(syn::Error::new_spanned(
            attr,
            "#[typed(...)] does not apply to brand wrappers",
        )),
        None => Ok(()),
    }
}

pub fn derive_typed_serde(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if let Some(t) = crate::brand::brand_param(&input) {
        return crate::brand::expand_serde(&input.ident, &t).into();
    }
    let result = match &input.data {
        syn::Data::Enum(_) => {
            if is_tag_enum(&input) {
                expand_tag_serde(&input)
            } else {
                expand_enum_serde(&input)
            }
        }
        _ => expand_facet(&input, expand_serde),
    };
    match result {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

pub fn derive_typed_sqlx(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if let Some(t) = crate::brand::brand_param(&input) {
        return crate::brand::expand_sqlx(&input.ident, &t).into();
    }
    let result = match &input.data {
        syn::Data::Enum(_) => {
            if is_tag_enum(&input) {
                Err(syn::Error::new_spanned(
                    &input.ident,
                    "TypedSqlx does not apply to tag enums (their variants map to distinct columns)",
                ))
            } else {
                expand_enum_sqlx(&input)
            }
        }
        _ => expand_facet(&input, expand_sqlx),
    };
    match result {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

pub fn derive_typed_openapi(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if let Some(t) = crate::brand::brand_param(&input) {
        return crate::brand::expand_openapi(&input.ident, &t).into();
    }
    let result = match &input.data {
        syn::Data::Enum(_) => {
            if is_tag_enum(&input) {
                expand_tag_openapi(&input)
            } else {
                expand_enum_openapi(&input)
            }
        }
        _ => expand_facet(&input, expand_openapi),
    };
    match result {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Tag enums have newtype variants carrying `#[tag(...)]`; plain typed enums
/// have unit variants.
fn is_tag_enum(input: &DeriveInput) -> bool {
    match &input.data {
        syn::Data::Enum(data) => data
            .variants
            .iter()
            .any(|v| matches!(v.fields, syn::Fields::Unnamed(_))),
        _ => false,
    }
}

fn expand_facet(
    input: &DeriveInput,
    facet: fn(&Ident, &Type, &Kind, &TypedArgs) -> proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let inner = inner_field(input)?;
    let kind = detect_kind(inner)?;
    let args = parse_args(input)?;
    Ok(facet(&input.ident, inner, &kind, &args))
}

#[derive(Debug, Default, FromMeta)]
struct TypedArgs {
    #[darling(default)]
    secret: bool,
    #[darling(default, with = preserve_str_literal, map = Some)]
    default: Option<Expr>,
    len: Option<Expr>,
    min_len: Option<Expr>,
    max_len: Option<Expr>,
    #[darling(default, with = preserve_str_literal, map = Some)]
    regex: Option<Expr>,
    parser: Option<Expr>,
    expected: Option<String>,
    min: Option<Expr>,
    max: Option<Expr>,
    #[darling(multiple)]
    try_from: Vec<Ident>,
    /// No construction door at all: no `Typed`, no `FromStr`, no `TryFrom`,
    /// no `Default`. The type hand-writes its only constructors; the derive
    /// gives it the facets a stored value needs.
    #[darling(default)]
    sealed: bool,
}

/// The wrapped primitive, detected from the single unnamed field's type.
enum Kind {
    Str,
    Numeric,
    Bool,
    Id,
    IdL(Ident),
    Timestamp,
    IntervalEnd,
    Composed,
}

fn parse_args(input: &DeriveInput) -> syn::Result<TypedArgs> {
    let mut typed_attrs = input.attrs.iter().filter(|a| a.path().is_ident("typed"));
    let Some(attr) = typed_attrs.next() else {
        return Ok(TypedArgs::default());
    };
    if typed_attrs.next().is_some() {
        return Err(syn::Error::new_spanned(
            attr,
            "at most one #[typed(...)] attribute is allowed",
        ));
    }
    let nested =
        darling::ast::NestedMeta::parse_meta_list(attr.meta.require_list()?.tokens.clone())
            .map_err(|e| syn::Error::new_spanned(attr, e))?;
    TypedArgs::from_list(&nested).map_err(|e| syn::Error::new_spanned(attr, e))
}

fn inner_field(input: &DeriveInput) -> syn::Result<&Type> {
    let err = || {
        syn::Error::new_spanned(
            &input.ident,
            "Typed derives require a tuple struct with exactly one field, e.g. `pub struct Foo(String);`",
        )
    };
    match &input.data {
        syn::Data::Struct(s) => match &s.fields {
            syn::Fields::Unnamed(f) if f.unnamed.len() == 1 => Ok(&f.unnamed.first().unwrap().ty),
            _ => Err(err()),
        },
        _ => Err(err()),
    }
}

fn detect_kind(inner: &Type) -> syn::Result<Kind> {
    let Type::Path(p) = inner else {
        return Ok(Kind::Composed);
    };
    let segment = p.path.segments.last().unwrap();
    Ok(match segment.ident.to_string().as_str() {
        "String" => Kind::Str,
        "i16" | "i32" | "i64" | "f32" | "f64" => Kind::Numeric,
        "bool" => Kind::Bool,
        "Id" => Kind::Id,
        "IdL" => {
            let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
                return Err(syn::Error::new_spanned(
                    segment,
                    "IdL requires a key type argument",
                ));
            };
            match args.args.first() {
                Some(syn::GenericArgument::Type(Type::Path(k))) if k.path.get_ident().is_some() => {
                    Kind::IdL(k.path.get_ident().unwrap().clone())
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        segment,
                        "IdL key must be a bare ident; the key struct is generated by the derive",
                    ));
                }
            }
        }
        "Timestamp" => Kind::Timestamp,
        "IntervalEnd" => Kind::IntervalEnd,
        _ => Kind::Composed,
    })
}

fn validate_args(name: &Ident, kind: &Kind, args: &TypedArgs) -> syn::Result<()> {
    let err = |what: &str, allowed: &str| {
        Err(syn::Error::new_spanned(
            name,
            format!("#[typed({what} = ...)] only applies to {allowed} newtypes"),
        ))
    };
    if args.sealed {
        let alone = !args.secret
            && args.default.is_none()
            && args.len.is_none()
            && args.min_len.is_none()
            && args.max_len.is_none()
            && args.regex.is_none()
            && args.parser.is_none()
            && args.expected.is_none()
            && args.min.is_none()
            && args.max.is_none()
            && args.try_from.is_empty();
        if !alone || matches!(kind, Kind::Composed) {
            return Err(syn::Error::new_spanned(
                name,
                "#[typed(sealed)] stands alone, on a primitive newtype",
            ));
        }
        return Ok(());
    }
    if !matches!(kind, Kind::Str) {
        if args.secret {
            return err("secret", "String");
        }
        if args.len.is_some() || args.min_len.is_some() || args.max_len.is_some() {
            return err("len/min_len/max_len", "String");
        }
        if args.regex.is_some() {
            return err("regex", "String");
        }
        if args.parser.is_some() {
            return err("parser", "String");
        }
        if args.expected.is_some() {
            return err("expected", "String");
        }
    }
    if args.expected.is_some() && args.regex.is_none() {
        return Err(syn::Error::new_spanned(
            name,
            "#[typed(expected = ...)] requires #[typed(regex = ...)]",
        ));
    }
    if !matches!(kind, Kind::Numeric) && (args.min.is_some() || args.max.is_some()) {
        return err("min/max", "numeric");
    }
    if matches!(kind, Kind::IdL(_) | Kind::Composed) && args.default.is_some() {
        return err("default", "String/numeric/bool/Id/Timestamp");
    }

    // Cross-checks, only when both sides are plain literals.
    let as_usize = |e: &Option<Expr>| -> Option<usize> {
        match e {
            Some(Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(i),
                ..
            })) => i.base10_parse().ok(),
            _ => None,
        }
    };
    let as_f64 = |e: &Option<Expr>| -> Option<f64> {
        match e {
            Some(Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(i),
                ..
            })) => i.base10_parse().ok(),
            Some(Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Float(f),
                ..
            })) => f.base10_parse().ok(),
            _ => None,
        }
    };
    let lit_err = |msg: &str| Err(syn::Error::new_spanned(name, msg.to_string()));
    let (len, min_len, max_len) = (
        as_usize(&args.len),
        as_usize(&args.min_len),
        as_usize(&args.max_len),
    );
    if let (Some(l), Some(mn)) = (len, min_len)
        && l < mn
    {
        return lit_err("len must be >= min_len");
    }
    if let (Some(l), Some(mx)) = (len, max_len)
        && l > mx
    {
        return lit_err("len must be <= max_len");
    }
    if let (Some(mn), Some(mx)) = (min_len, max_len)
        && mn > mx
    {
        return lit_err("min_len must be <= max_len");
    }
    if let Some(Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(d),
        ..
    })) = &args.default
    {
        // Characters, matching the runtime checks and PostgreSQL varchar(n).
        let d_len = d.value().chars().count();
        if let Some(l) = len
            && d_len != l
        {
            return lit_err("default length must equal len");
        }
        if let Some(mn) = min_len
            && d_len < mn
        {
            return lit_err("default length must be >= min_len");
        }
        if let Some(mx) = max_len
            && d_len > mx
        {
            return lit_err("default length must be <= max_len");
        }
    }
    let (min, max, default) = (as_f64(&args.min), as_f64(&args.max), as_f64(&args.default));
    if let (Some(mn), Some(mx)) = (min, max)
        && mn > mx
    {
        return lit_err("min must be <= max");
    }
    if let Some(d) = default {
        if let Some(mn) = min
            && d < mn
        {
            return lit_err("default must be >= min");
        }
        if let Some(mx) = max
            && d > mx
        {
            return lit_err("default must be <= max");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `Typed` — the domain core
// ---------------------------------------------------------------------------

fn expand_typed(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let inner = inner_field(input)?;
    let kind = detect_kind(inner)?;
    let args = parse_args(input)?;
    validate_args(name, &kind, &args)?;

    let debug = if args.secret {
        impl_redacted_debug(name)
    } else {
        impl_compact_debug(name)
    };

    if args.sealed {
        let clone = impl_clone(name);
        let body = expand_sealed(name, inner, &kind);
        return Ok(quote! { #clone #debug #body });
    }

    let gate = impl_gate(name, inner, &kind, &args);
    let body = match &kind {
        Kind::Composed => expand_composed(name, inner),
        _ => expand_primitive(name, inner, &kind, &args),
    };

    // TryFrom sibling newtypes: converts by re-gating the sibling's inner
    // value (composed re-parses the composed string, converting the
    // sibling's name types).
    let sibling_gate = if matches!(kind, Kind::Composed) {
        quote! { ta_objects::ComposedString::parse(val.0) }
    } else {
        quote! { <Self as ta_objects::Typed>::try_new(val.0) }
    };
    let try_froms = args.try_from.iter().map(|from| {
        quote! {
            impl TryFrom<#from> for #name {
                type Error = td_error::TdError;
                fn try_from(val: #from) -> Result<Self, Self::Error> { #sibling_gate }
            }
            impl TryFrom<&#from> for #name {
                type Error = td_error::TdError;
                fn try_from(val: &#from) -> Result<Self, Self::Error> {
                    Self::try_from(val.clone())
                }
            }
        }
    });

    let clone = impl_clone(name);
    Ok(quote! { #clone #debug #gate #body #(#try_froms)* })
}

/// The construction gate: `impl ta_objects::Typed`, applying the configured
/// `#[typed(...)]` constraints.
fn impl_gate(
    name: &Ident,
    inner: &Type,
    kind: &Kind,
    args: &TypedArgs,
) -> proc_macro2::TokenStream {
    let checks: Vec<proc_macro2::TokenStream> = match kind {
        Kind::Str => {
            let mut checks = Vec::new();
            if let Some(l) = &args.len {
                checks.push(quote! { ta_objects::typed::check_len(stringify!(#name), &val, #l)?; });
            }
            if let Some(mn) = &args.min_len {
                checks.push(
                    quote! { ta_objects::typed::check_min_len(stringify!(#name), &val, #mn)?; },
                );
            }
            if let Some(mx) = &args.max_len {
                checks.push(
                    quote! { ta_objects::typed::check_max_len(stringify!(#name), &val, #mx)?; },
                );
            }
            if let Some(re) = &args.regex {
                if let Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit),
                    ..
                }) = re
                    && let Err(e) = regex::Regex::new(&lit.value())
                {
                    return syn::Error::new_spanned(re, format!("invalid regex: {e}"))
                        .to_compile_error();
                }
                let violation = match &args.expected {
                    Some(msg) => quote! {
                        ta_objects::typed::TypedError::Expected(
                            stringify!(#name), val.clone(), #msg,
                        )
                    },
                    None => quote! {
                        ta_objects::typed::TypedError::Regex(
                            stringify!(#name), val.clone(), RE.to_string(),
                        )
                    },
                };
                checks.push(quote! {
                    {
                        static RE: std::sync::LazyLock<regex::Regex> =
                            std::sync::LazyLock::new(|| regex::Regex::new(&#re).unwrap());
                        if !RE.is_match(&val) {
                            Err(#violation)?;
                        }
                    }
                });
            }
            if let Some(parser) = &args.parser {
                checks.push(quote! { let val = ta_objects::typed::apply_parser(#parser, val)?; });
            }
            checks
        }
        Kind::Numeric => {
            let mut checks = Vec::new();
            if let Some(mn) = &args.min {
                checks
                    .push(quote! { ta_objects::typed::check_min(stringify!(#name), &val, #mn)?; });
            }
            if let Some(mx) = &args.max {
                checks
                    .push(quote! { ta_objects::typed::check_max(stringify!(#name), &val, #mx)?; });
            }
            checks
        }
        _ => Vec::new(),
    };

    quote! {
        impl ta_objects::Typed for #name {
            type Inner = #inner;
            fn try_new(val: impl Into<Self::Inner>) -> Result<Self, td_error::TdError> {
                let val = val.into();
                #(#checks)*
                Ok(Self(val))
            }
            fn into_inner(self) -> Self::Inner { self.0 }
        }
    }
}

fn expand_primitive(
    name: &Ident,
    inner: &Type,
    kind: &Kind,
    args: &TypedArgs,
) -> proc_macro2::TokenStream {
    let copy = matches!(
        kind,
        Kind::Numeric | Kind::Bool | Kind::Id | Kind::Timestamp | Kind::IntervalEnd
    )
    .then(|| impl_copy(name));
    let identity = matches!(kind, Kind::Id | Kind::IdL(_)).then(|| impl_identity(name));

    // Default only where it can be valid: an explicit `default = ...`
    // (gated at construction; literal cross-checks are the compile-time
    // fast path), or a kind with no constraints configured. Constrained
    // types without an explicit default get no Default impl at all.
    let constrained = args.min.is_some() || args.max.is_some();
    let default_expr = match (&kind, &args.default) {
        (Kind::Str, None) | (Kind::IdL(_), _) => None,
        (_, Some(d)) => Some(quote! { #d }),
        (Kind::Numeric, None) if constrained => None,
        (_, None) => Some(quote! { <#inner as Default>::default() }),
    };
    let default = default_expr.map(|d| {
        quote! {
            impl Default for #name {
                fn default() -> Self {
                    <Self as ta_objects::Typed>::try_new(#d)
                        .expect(concat!("invalid default for ", stringify!(#name)))
                }
            }
        }
    });

    let appendix = match kind {
        Kind::Numeric => quote! {
            impl TryFrom<#inner> for #name {
                type Error = td_error::TdError;
                fn try_from(v: #inner) -> Result<Self, Self::Error> {
                    <Self as ta_objects::Typed>::try_new(v)
                }
            }
        },
        Kind::Bool => quote! {
            impl #name {
                pub const TRUE: Self = Self(true);
                pub const FALSE: Self = Self(false);
            }
            impl From<#inner> for #name {
                fn from(v: #inner) -> Self { Self(v) }
            }
        },
        Kind::Id => quote! {
            impl From<#inner> for #name { fn from(v: #inner) -> Self { Self(v) } }
        },
        Kind::IdL(key) => expand_idl_appendix(name, inner, key),
        Kind::Timestamp => quote! {
            impl #name {
                pub fn now() -> Self {
                    Self(td_common::datetime::Timestamp::from(td_common::time::UniqueUtc::now_millis()))
                }
            }
            impl From<#inner> for #name {
                fn from(v: #inner) -> Self { Self(v) }
            }
            impl From<chrono::DateTime<chrono::Utc>> for #name {
                fn from(v: chrono::DateTime<chrono::Utc>) -> Self { Self(<#inner>::from(v)) }
            }
        },
        Kind::IntervalEnd => quote! {
            impl From<#inner> for #name {
                fn from(v: #inner) -> Self { Self(v) }
            }
            impl From<td_common::datetime::Timestamp> for #name {
                fn from(v: td_common::datetime::Timestamp) -> Self { Self(<#inner>::from(v)) }
            }
        },
        _ => quote! {},
    };

    let sea_value = impl_sea_value_typed(name, inner);
    // An interval end's Infinity has no bindable value: as a condition leaf
    // it would render `= NULL` and match nothing, so it gets none.
    let sql_condition = match kind {
        Kind::IntervalEnd => quote! {},
        _ => impl_sql_condition(name),
    };
    let common = impl_common_traits(name, inner, args.secret);
    let try_from_str = impl_try_from_str(name);

    quote! {
        #copy
        #identity
        #default
        #appendix

        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                ta_objects::typed::from_str_typed(s)
            }
        }

        #sea_value
        #sql_condition
        #common
        #try_from_str
    }
}

/// `#[typed(sealed)]`: the facets of a stored value — value, condition leaf,
/// Deref, Display — and nothing that constructs one.
fn expand_sealed(name: &Ident, inner: &Type, kind: &Kind) -> proc_macro2::TokenStream {
    let copy = matches!(
        kind,
        Kind::Numeric | Kind::Bool | Kind::Id | Kind::Timestamp | Kind::IntervalEnd
    )
    .then(|| impl_copy(name));
    let sea_value = impl_sea_value_typed(name, inner);
    let sql_condition = match kind {
        Kind::IntervalEnd => quote! {},
        _ => impl_sql_condition(name),
    };
    quote! {
        #copy
        #sea_value
        #sql_condition
        impl std::ops::Deref for #name {
            type Target = #inner;
            fn deref(&self) -> &Self::Target { &self.0 }
        }
        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                Ok(Self(<#inner as ta_objects::TypedInner>::parse_str(s)?))
            }
            fn as_string(&self) -> String {
                self.to_string()
            }
        }
    }
}

fn expand_idl_appendix(name: &Ident, inner: &Type, key: &Ident) -> proc_macro2::TokenStream {
    let gen_var = format_ident!("{}_GENERATOR", name.to_string().to_uppercase());
    // The logical ID prefix is always lowercase, regardless of the key's struct-tag casing.
    let key_str = key.to_string().to_ascii_lowercase();
    let uninit_msg = format!(
        "{name} generator not initialised — call {name}::init() at startup before generating IDs"
    );

    quote! {
        #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct #key;

        impl td_common::logic_id::KeyId for #key {
            fn key() -> &'static str { #key_str }
        }

        static #gen_var: std::sync::OnceLock<td_common::logic_id::IdLGenerator<#key>> =
            std::sync::OnceLock::new();

        impl #name {
            pub fn init(latest: Option<#name>) -> Result<(), td_error::TdError> {
                let latest = latest.map(|id| id.0);
                let generator = td_common::logic_id::IdLGenerator::<#key>::new(&latest)?;
                #gen_var.set(generator).map_err(|_| td_error::api_error!(
                    td_error::ApiError::InputError,
                    concat!(stringify!(#name), " generator already initialized")
                ))
            }

            pub fn new() -> Self {
                #[cfg(feature = "td-test")]
                let generator = #gen_var.get_or_init(|| {
                    td_common::logic_id::IdLGenerator::<#key>::new(&None).unwrap()
                });
                #[cfg(not(feature = "td-test"))]
                let generator = #gen_var.get().expect(#uninit_msg);
                generator.generate().into()
            }
        }

        impl From<#inner> for #name { fn from(v: #inner) -> Self { Self(v) } }
    }
}

fn expand_composed(name: &Ident, inner: &Type) -> proc_macro2::TokenStream {
    let try_from_str = impl_try_from_str(name);
    let sql_condition = impl_sql_condition(name);

    quote! {
        impl #name {
            pub fn new(val: impl Into<#inner>) -> Self { Self(val.into()) }
        }

        impl ta_objects::ComposedString for #name {
            fn parse(s: impl Into<String>) -> Result<Self, td_error::TdError> {
                use ta_objects::ComposedString;
                Ok(Self::new(<#inner>::parse(s)?))
            }
            fn compose(&self) -> String { self.0.compose() }
        }

        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                ta_objects::ComposedString::parse(s)
            }
        }

        impl From<#inner> for #name {
            fn from(v: #inner) -> #name { Self::new(v) }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                use ta_objects::ComposedString;
                f.write_str(&self.compose())
            }
        }

        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                use ta_objects::ComposedString;
                sea_query::Value::String(Some(self.compose()))
            }
        }
        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value { sea_query::Value::String(None) }
        }

        #sql_condition

        impl std::ops::Deref for #name {
            type Target = #inner;
            fn deref(&self) -> &Self::Target { &self.0 }
        }

        #try_from_str
    }
}

// ---------------------------------------------------------------------------
// `TypedSerde` — the wire contract
// ---------------------------------------------------------------------------

fn expand_serde(
    name: &Ident,
    inner: &Type,
    kind: &Kind,
    args: &TypedArgs,
) -> proc_macro2::TokenStream {
    if args.sealed {
        return quote! {
            impl<'de> serde::Deserialize<'de> for #name {
                fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                    Ok(Self(<#inner as serde::Deserialize>::deserialize(d)?))
                }
            }
            impl serde::Serialize for #name {
                fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                    serde::Serialize::serialize(&self.0, s)
                }
            }
        };
    }
    if matches!(kind, Kind::Composed) {
        return quote! {
            impl<'de> serde::Deserialize<'de> for #name {
                fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                    use ta_objects::ComposedString;
                    String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
                }
            }
            impl serde::Serialize for #name {
                fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                    use ta_objects::ComposedString;
                    self.compose().serialize(s)
                }
            }
        };
    }
    quote! {
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                ta_objects::typed::deserialize_typed(d)
            }
        }
        impl serde::Serialize for #name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(s)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// `TypedSqlx` — the database contract
// ---------------------------------------------------------------------------

fn expand_sqlx(
    name: &Ident,
    inner: &Type,
    kind: &Kind,
    args: &TypedArgs,
) -> proc_macro2::TokenStream {
    let decode = if args.sealed {
        quote! { Ok(Self(<#inner as sqlx::decode::Decode<'r, DB>>::decode(value)?)) }
    } else {
        quote! { ta_objects::typed::decode_typed::<DB, Self>(value) }
    };
    if matches!(kind, Kind::Composed) {
        // String bridge: decode re-parses the composed string (gated).
        return quote! {
            impl<DB: sqlx::Database> sqlx::Type<DB> for #name
            where
                String: sqlx::Type<DB>,
            {
                fn type_info() -> <DB as sqlx::Database>::TypeInfo {
                    <String as sqlx::Type<DB>>::type_info()
                }
            }

            impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for #name
            where
                String: sqlx::Decode<'r, DB>,
            {
                fn decode(
                    value: <DB as sqlx::Database>::ValueRef<'r>,
                ) -> Result<Self, sqlx::error::BoxDynError> {
                    let decoded = <String as sqlx::Decode<'r, DB>>::decode(value)?;
                    Ok(Self::try_from(decoded).map_err(Box::new)?)
                }
            }
        };
    }
    // Equivalent of `#[derive(sqlx::Type)] #[sqlx(transparent)]`, except
    // Decode routes through the gate.
    quote! {
        impl<DB: sqlx::Database> sqlx::Type<DB> for #name
        where
            #inner: sqlx::Type<DB>,
        {
            fn type_info() -> <DB as sqlx::Database>::TypeInfo {
                <#inner as sqlx::Type<DB>>::type_info()
            }
            fn compatible(ty: &<DB as sqlx::Database>::TypeInfo) -> bool {
                <#inner as sqlx::Type<DB>>::compatible(ty)
            }
        }
        impl<'q, DB: sqlx::Database> sqlx::encode::Encode<'q, DB> for #name
        where
            #inner: sqlx::encode::Encode<'q, DB>,
        {
            fn encode_by_ref(
                &self,
                buf: &mut <DB as sqlx::Database>::ArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <#inner as sqlx::encode::Encode<'q, DB>>::encode_by_ref(&self.0, buf)
            }
            fn produces(&self) -> Option<<DB as sqlx::Database>::TypeInfo> {
                <#inner as sqlx::encode::Encode<'q, DB>>::produces(&self.0)
            }
            fn size_hint(&self) -> usize {
                <#inner as sqlx::encode::Encode<'q, DB>>::size_hint(&self.0)
            }
        }
        impl<'r, DB: sqlx::Database> sqlx::decode::Decode<'r, DB> for #name
        where
            #inner: sqlx::decode::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                #decode
            }
        }
        impl sqlx::postgres::PgHasArrayType for #name
        where
            #inner: sqlx::postgres::PgHasArrayType,
        {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <#inner as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// `TypedOpenApi` — the API docs contract
// ---------------------------------------------------------------------------

fn expand_openapi(
    name: &Ident,
    inner: &Type,
    kind: &Kind,
    args: &TypedArgs,
) -> proc_macro2::TokenStream {
    let schema_expr = openapi_schema_expr(inner, kind, args);
    impl_utoipa(name, &schema_expr)
}

/// The schema expression: the inner's base schema, enriched with the
/// `#[typed(...)]` constraints the gate enforces (so the docs state the
/// actual validation rules). `parser` constraints are unrepresentable.
fn openapi_schema_expr(inner: &Type, kind: &Kind, args: &TypedArgs) -> proc_macro2::TokenStream {
    let constrained = args.secret
        || args.len.is_some()
        || args.min_len.is_some()
        || args.max_len.is_some()
        || args.regex.is_some()
        || args.min.is_some()
        || args.max.is_some();
    if !constrained {
        return if matches!(kind, Kind::Composed) {
            quote! { <String as utoipa::PartialSchema>::schema() }
        } else {
            quote! { <#inner as ta_objects::TypedInner>::schema() }
        };
    }
    match kind {
        Kind::Str => {
            let mut chain = quote! {
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::SchemaType::new(
                        utoipa::openapi::schema::Type::String))
            };
            if let Some(l) = &args.len {
                chain.extend(quote! { .min_length(Some(#l)).max_length(Some(#l)) });
            }
            if let Some(v) = &args.min_len {
                chain.extend(quote! { .min_length(Some(#v)) });
            }
            if let Some(v) = &args.max_len {
                chain.extend(quote! { .max_length(Some(#v)) });
            }
            if let Some(re) = &args.regex {
                chain.extend(quote! { .pattern(Some(#re)) });
            }
            if args.secret {
                chain.extend(quote! {
                    .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                        utoipa::openapi::schema::KnownFormat::Password)))
                });
            }
            quote! { #chain.into() }
        }
        Kind::Numeric => {
            let prim = match inner {
                Type::Path(p) => p.path.segments.last().unwrap().ident.to_string(),
                _ => unreachable!(),
            };
            let (ty, format) = match prim.as_str() {
                "i16" | "i32" => (quote! { Integer }, quote! { Int32 }),
                "i64" => (quote! { Integer }, quote! { Int64 }),
                "f32" => (quote! { Number }, quote! { Float }),
                _ => (quote! { Number }, quote! { Double }),
            };
            let mut chain = quote! {
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::SchemaType::new(
                        utoipa::openapi::schema::Type::#ty))
                    .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                        utoipa::openapi::schema::KnownFormat::#format)))
            };
            if let Some(v) = &args.min {
                chain.extend(quote! { .minimum(Some(#v)) });
            }
            if let Some(v) = &args.max {
                chain.extend(quote! { .maximum(Some(#v)) });
            }
            quote! { #chain.into() }
        }
        _ => quote! { <#inner as ta_objects::TypedInner>::schema() },
    }
}

// ---------------------------------------------------------------------------
// Shared codegen helpers
// ---------------------------------------------------------------------------

fn impl_clone(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl Clone for #name {
            fn clone(&self) -> Self { Self(self.0.clone()) }
        }
    }
}

fn impl_copy(name: &Ident) -> proc_macro2::TokenStream {
    quote! { impl Copy for #name {} }
}

fn impl_identity(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl PartialEq for #name {
            fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
        }
        impl Eq for #name {}
        impl PartialOrd for #name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for #name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.0.cmp(&other.0) }
        }
        impl std::hash::Hash for #name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.0.hash(state) }
        }
    }
}

/// Unlike `#[derive(Debug)]`, does NOT expand to multi-line with `{:#?}`.
fn impl_compact_debug(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.0, f)
            }
        }
    }
}

fn impl_redacted_debug(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "***")
            }
        }
    }
}

fn impl_sea_value_typed(name: &Ident, inner: &Type) -> proc_macro2::TokenStream {
    quote! {
        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                ta_objects::TypedInner::sea_value(&self.0)
            }
        }
        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value {
                <#inner as ta_objects::TypedInner>::null_sea_value()
            }
        }
    }
}

/// Emits `SqlCondition<D>` for a leaf type (single AND with one column),
/// implemented only for DAOs that have a column of this type.
fn impl_sql_condition(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl<D: ta_objects::HasColumn<#name>> ta_objects::sql_condition::SqlCondition<D> for #name {
            fn resolve(&self) -> sea_query::Condition {
                ta_objects::sql_condition::resolve_leaf_eq::<D, Self>(self)
            }

            fn display_values(&self) -> Vec<(String, String)> {
                ta_objects::sql_condition::leaf_display_values::<D, Self>(self)
            }
        }
    }
}

/// Emits Deref, Display, ListFilterStr. `secret` types get neither Display
/// nor ListFilterStr (rendering a secret must be a visible choice) — they
/// get an explicit `expose()` instead.
fn impl_common_traits(name: &Ident, inner: &Type, secret: bool) -> proc_macro2::TokenStream {
    let deref = quote! {
        impl std::ops::Deref for #name {
            type Target = #inner;
            fn deref(&self) -> &Self::Target { &self.0 }
        }
    };
    if secret {
        return quote! {
            #deref
            impl #name {
                /// Deliberate access to the secret value; every call site is
                /// an explicit decision to render it.
                pub fn expose(&self) -> &str { &self.0 }
            }
        };
    }
    quote! {
        #deref
        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                use std::str::FromStr;
                #name::from_str(s).map_err(Into::into)
            }
            fn as_string(&self) -> String {
                self.to_string()
            }
        }
    }
}

/// Emits TryFrom<String>, TryFrom<&str> via FromStr. Signatures use the
/// concrete error type, not `Self::Error` — an enum may have an `Error`
/// variant, making the associated-item path ambiguous.
fn impl_try_from_str(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl TryFrom<String> for #name {
            type Error = td_error::TdError;
            fn try_from(v: String) -> Result<Self, td_error::TdError> { v.parse() }
        }
        impl TryFrom<&str> for #name {
            type Error = td_error::TdError;
            fn try_from(v: &str) -> Result<Self, td_error::TdError> { v.parse() }
        }
    }
}

fn impl_utoipa(name: &Ident, schema_expr: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    quote! {
        impl utoipa::__dev::ComposeSchema for #name {
            fn compose(_: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>)
                -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                #schema_expr
            }
        }
        impl utoipa::ToSchema for #name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!(#name))
            }
            fn schemas(_: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>)>) {}
        }
    }
}

// ---------------------------------------------------------------------------
// `#[derive(TypedEnum)]` — closed string set (unit variants)
// ---------------------------------------------------------------------------
// The canonical string of a variant is its `#[typed_enum(rename = "…")]` or
// its ident. `Display` renders the canonical form; `FromStr` (the gate for a
// closed set) accepts the ident and every rename.

struct EnumVariant {
    ident: Ident,
    canonical: String,
    renames: Vec<String>,
}

fn plain_enum_variants(input: &DeriveInput) -> syn::Result<Vec<EnumVariant>> {
    let syn::Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TypedEnum requires an enum",
        ));
    };
    let mut out = Vec::new();
    for v in &data.variants {
        if !matches!(v.fields, syn::Fields::Unit) {
            return Err(syn::Error::new_spanned(
                &v.ident,
                "TypedEnum variants must be unit variants",
            ));
        }
        let renames: Vec<String> = v
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("typed_enum"))
            .filter_map(|a| a.parse_args::<syn::MetaNameValue>().ok())
            .filter(|mnv| mnv.path.is_ident("rename"))
            .filter_map(|mnv| {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = mnv.value
                {
                    Some(s.value())
                } else {
                    None
                }
            })
            .collect();
        let canonical = renames
            .first()
            .cloned()
            .unwrap_or_else(|| v.ident.to_string());
        out.push(EnumVariant {
            ident: v.ident.clone(),
            canonical,
            renames,
        });
    }
    Ok(out)
}

pub fn derive_typed_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_typed_enum(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_typed_enum(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let variants = plain_enum_variants(input)?;

    let idents: Vec<&Ident> = variants.iter().map(|v| &v.ident).collect();
    let ident_strs: Vec<String> = variants.iter().map(|v| v.ident.to_string()).collect();
    let canonicals: Vec<&String> = variants.iter().map(|v| &v.canonical).collect();

    // FromStr accepts the renames when present, else the ident — a rename
    // REPLACES the ident as the string form (parsing is case-sensitive).
    let from_str_arms = variants.iter().map(|v| {
        let ident = &v.ident;
        let ident_str = v.ident.to_string();
        let renames = &v.renames;
        if renames.is_empty() {
            quote! { #ident_str => Ok(Self::#ident), }
        } else {
            quote! { #(#renames)|* => Ok(Self::#ident), }
        }
    });

    let sql_condition = impl_sql_condition(name);
    let try_from_str = impl_try_from_str(name);

    Ok(quote! {
        #try_from_str

        impl Clone for #name {
            fn clone(&self) -> Self {
                match self { #(Self::#idents => Self::#idents,)* }
            }
        }

        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(match self { #(Self::#idents => #ident_strs,)* })
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(match self { #(Self::#idents => #canonicals,)* })
            }
        }

        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            // `td_error::TdError`, not `Self::Err`: an enum may have an `Err`
            // variant, making `Self::Err` ambiguous.
            fn from_str(s: &str) -> Result<Self, td_error::TdError> {
                match s {
                    #(#from_str_arms)*
                    _ => Err(ta_objects::typed::TypedError::UnknownVariant(
                        stringify!(#name), s.to_owned(),
                    ).into()),
                }
            }
        }

        impl #name {
            pub const ALL: &'static [Self] = &[#(Self::#idents),*];

            pub fn iter() -> impl Iterator<Item = Self> {
                Self::ALL.iter().cloned()
            }
        }

        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                sea_query::Value::String(Some(self.to_string()))
            }
        }

        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value {
                sea_query::Value::String(None)
            }
        }

        #sql_condition

        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                s.parse()
            }
            fn as_string(&self) -> String {
                self.to_string()
            }
        }
    })
}

/// The serde facet for plain enums: serialize the canonical form; accept the
/// canonical form plus the case-insensitive ergonomics (lowercased ident and
/// lowercased renames) — NOT the raw ident when renamed.
fn expand_enum_serde(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let variants = plain_enum_variants(input)?;

    let idents: Vec<&Ident> = variants.iter().map(|v| &v.ident).collect();
    let canonicals: Vec<&String> = variants.iter().map(|v| &v.canonical).collect();

    let de_arms = variants.iter().map(|v| {
        let ident = &v.ident;
        let ident_str = v.ident.to_string();
        let mut accepted: Vec<String> = vec![v.canonical.clone()];
        let ident_lower = ident_str.to_lowercase();
        if ident_lower != ident_str && !accepted.contains(&ident_lower) {
            accepted.push(ident_lower);
        }
        for r in &v.renames {
            let r_lower = r.to_lowercase();
            if r_lower != *r && !accepted.contains(&r_lower) {
                accepted.push(r_lower);
            }
        }
        quote! { #(#accepted)|* => Ok(Self::#ident), }
    });

    Ok(quote! {
        impl serde::Serialize for #name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(match self { #(Self::#idents => #canonicals,)* })
            }
        }
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                match s.as_str() {
                    #(#de_arms)*
                    _ => Err(serde::de::Error::custom(
                        ta_objects::typed::TypedError::UnknownVariant(
                            stringify!(#name), s,
                        ),
                    )),
                }
            }
        }
    })
}

/// The OpenAPI facet for plain enums: a string schema enumerating the
/// canonical values.
fn expand_enum_openapi(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let variants = plain_enum_variants(input)?;
    let canonicals: Vec<&String> = variants.iter().map(|v| &v.canonical).collect();
    let schema_expr = quote! {
        utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::schema::SchemaType::new(
                utoipa::openapi::schema::Type::String))
            .enum_values(Some([#(#canonicals),*]))
            .into()
    };
    Ok(impl_utoipa(name, &schema_expr))
}

/// The sqlx facet for plain enums: String bridge, decoding through FromStr
/// (the closed-set gate).
fn expand_enum_sqlx(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    plain_enum_variants(input)?;
    Ok(quote! {
        impl<DB: sqlx::Database> sqlx::Type<DB> for #name
        where
            String: sqlx::Type<DB>,
        {
            fn type_info() -> <DB as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<DB>>::type_info()
            }
            fn compatible(ty: &<DB as sqlx::Database>::TypeInfo) -> bool {
                <String as sqlx::Type<DB>>::compatible(ty)
            }
        }
        impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for #name
        where
            String: sqlx::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let decoded = <String as sqlx::Decode<'r, DB>>::decode(value)?;
                use std::str::FromStr;
                Ok(Self::from_str(&decoded).map_err(Box::new)?)
            }
        }
    })
}

// ---------------------------------------------------------------------------
// `#[derive(TypedTag)]` — newtype variants distinguished by a string prefix
// ---------------------------------------------------------------------------
// `Display` = tag + inner; `FromStr` strips the tag, longer tags tried first;
// the empty-tag variant (if any) is the catch-all.

struct TagVariant {
    ident: Ident,
    tag: String,
    ty: Type,
}

fn tag_enum_variants(input: &DeriveInput) -> syn::Result<Vec<TagVariant>> {
    let syn::Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TypedTag requires an enum",
        ));
    };
    let mut out = Vec::new();
    for v in &data.variants {
        let tag = v
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("tag"))
            .find_map(|a| a.parse_args::<syn::LitStr>().ok().map(|s| s.value()))
            .ok_or_else(|| {
                syn::Error::new_spanned(
                    &v.ident,
                    format!(
                        "TypedTag: variant `{}` must have a `#[tag(\"...\")]` attribute",
                        v.ident
                    ),
                )
            })?;
        let ty = match &v.fields {
            syn::Fields::Unnamed(f) if f.unnamed.len() == 1 => {
                f.unnamed.first().unwrap().ty.clone()
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &v.ident,
                    format!(
                        "TypedTag: variant `{}` must be a newtype (single unnamed field)",
                        v.ident
                    ),
                ));
            }
        };
        out.push(TagVariant {
            ident: v.ident.clone(),
            tag,
            ty,
        });
    }
    Ok(out)
}

pub fn derive_typed_tag(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_typed_tag(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_typed_tag(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let variants = tag_enum_variants(input)?;

    let idents: Vec<&Ident> = variants.iter().map(|v| &v.ident).collect();
    let ident_strs: Vec<String> = variants.iter().map(|v| v.ident.to_string()).collect();

    // `Tagged` for the inner type of each non-empty-tag variant: lets a caller
    // holding the inner id (e.g. a `ProjectId`) render it in this enum's id
    // spelling (`~<id>`) so `FromStr` parses it back as an id, not a name. The
    // empty-tag variant is skipped — it has no prefix, and its inner type often
    // coincides with the id variant's (which would collide).
    let tagged_impls = variants.iter().filter(|v| !v.tag.is_empty()).map(|v| {
        let (tag, ty) = (&v.tag, &v.ty);
        quote! {
            impl ta_objects::Tagged<#name> for #ty {
                const PREFIX: &'static str = #tag;
            }
        }
    });

    let display_arms = variants.iter().map(|v| {
        let (ident, tag, ty) = (&v.ident, &v.tag, &v.ty);
        if tag.is_empty() {
            quote! { Self::#ident(inner) => write!(f, "{}", inner), }
        } else {
            quote! {
                Self::#ident(inner) =>
                    write!(f, "{}{}", <#ty as ta_objects::Tagged<#name>>::PREFIX, inner),
            }
        }
    });

    let sea_value_arms = variants.iter().map(|v| {
        let ident = &v.ident;
        quote! { Self::#ident(inner) => inner.as_sea_value(), }
    });

    // FromStr: try longer tags first so the empty-tag variant is the catch-all.
    let mut sorted: Vec<usize> = (0..variants.len()).collect();
    sorted.sort_by(|&a, &b| variants[b].tag.len().cmp(&variants[a].tag.len()));
    let non_empty_checks = sorted
        .iter()
        .filter(|&&i| !variants[i].tag.is_empty())
        .map(|&i| {
            let v = &variants[i];
            let (ident, tag, ty) = (&v.ident, &v.tag, &v.ty);
            quote! {
                if let Some(rest) = s.strip_prefix(#tag) {
                    return Ok(Self::#ident(rest.parse::<#ty>()?));
                }
            }
        });
    let fallback = if let Some(&i) = sorted.iter().find(|&&i| variants[i].tag.is_empty()) {
        let v = &variants[i];
        let (ident, ty) = (&v.ident, &v.ty);
        quote! { Ok(Self::#ident(s.parse::<#ty>()?)) }
    } else {
        quote! {
            Err(ta_objects::typed::TypedError::NoTagMatch(
                stringify!(#name), s.to_owned(),
            ).into())
        }
    };

    // The active variant picks the column at runtime, so the DAO must have a
    // column for every variant's inner type.
    let variant_tys: Vec<&Type> = variants.iter().map(|v| &v.ty).collect();
    let resolve_arms = variants.iter().map(|v| {
        let (ident, ty) = (&v.ident, &v.ty);
        quote! {
            Self::#ident(inner) =>
                ta_objects::sql_condition::resolve_leaf_eq::<D, #ty>(inner),
        }
    });
    let display_value_arms = variants.iter().map(|v| {
        let (ident, ty) = (&v.ident, &v.ty);
        quote! {
            Self::#ident(inner) =>
                ta_objects::sql_condition::leaf_display_values::<D, #ty>(inner),
        }
    });
    // A tagged lookup addresses one row exactly when each variant's column
    // does — an id-or-name param is an identity lookup either way.
    let addresses_one_row = quote! {
        impl<D, __S> ta_objects::AddressesOneRow<D, __S> for #name
        where
            D: ta_objects::Dao,
            __S: td_database::access::DbScope,
            Self: ta_objects::sql_condition::SqlCondition<D>,
            #(#variant_tys: ta_objects::AddressesOneRow<D, __S>,)*
        {
        }
    };
    let sql_condition = quote! {
        impl<D> ta_objects::sql_condition::SqlCondition<D> for #name
        where
            D: ta_objects::Dao #(+ ta_objects::HasColumn<#variant_tys>)*,
        {
            fn resolve(&self) -> sea_query::Condition {
                match self {
                    #(#resolve_arms)*
                }
            }

            fn display_values(&self) -> Vec<(String, String)> {
                match self {
                    #(#display_value_arms)*
                }
            }
        }
    };

    Ok(quote! {
        #addresses_one_row

        impl Clone for #name {
            fn clone(&self) -> Self {
                match self { #(Self::#idents(inner) => Self::#idents(inner.clone()),)* }
            }
        }

        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(Self::#idents(inner) => f.debug_tuple(#ident_strs).field(inner).finish(),)*
                }
            }
        }

        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                #(#non_empty_checks)*
                #fallback
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(#display_arms)*
                }
            }
        }

        #(#tagged_impls)*

        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                match self {
                    #(#sea_value_arms)*
                }
            }
        }

        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value { sea_query::Value::String(None) }
        }

        #sql_condition

        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                s.parse()
            }
            fn as_string(&self) -> String { self.to_string() }
        }

        impl TryFrom<String> for #name {
            type Error = td_error::TdError;
            fn try_from(s: String) -> Result<Self, Self::Error> { s.parse() }
        }
        impl From<#name> for String {
            fn from(v: #name) -> String { v.to_string() }
        }
    })
}

fn expand_tag_serde(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    tag_enum_variants(input)?;
    Ok(quote! {
        impl serde::Serialize for #name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.to_string().serialize(s)
            }
        }
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
            }
        }
    })
}

fn expand_tag_openapi(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    tag_enum_variants(input)?;
    let schema_expr = quote! { <String as utoipa::PartialSchema>::schema() };
    Ok(impl_utoipa(name, &schema_expr))
}
