//
// Copyright 2025 Tabs Data Inc.
//

// The DaoType derive macro generates an Iden enum from struct field names.
// Given a struct field `defined_on`, the generated variant is `DefinedOn`
// (UpperCamelCase via heck). We must use the same conversion when referencing
// variants from the macro, otherwise the generated code won't compile.
//
// Field idents (snake_case) → Iden variants (UpperCamelCase):
//   id           → Id
//   defined_on   → DefinedOn
//   table_id     → TableId
//   partition_by → PartitionBy
//
// The Iden enum itself is named <StructName>Iden and has a leading Table variant:
//   struct TableDB { id, defined_on } → enum TableDBIden { Table, Id, DefinedOn }

use crate::type_builder::{parse_input_item_struct, td_type};
use darling::util::{Flag, PathList};
use darling::{FromAttributes, FromDeriveInput, FromMeta};
use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{ToTokens, format_ident, quote};
use std::collections::HashMap;
use syn::{DeriveInput, Expr, Fields, ItemStruct, Type, parse_macro_input};

// ---- Darling argument structs ----------------------------------------------

#[derive(Debug, FromDeriveInput, FromAttributes)]
#[darling(attributes(dao))]
pub struct DaoArguments {
    update: Flag,
    sql_table: Option<String>,
    order_by: Option<OrderByArguments>,
    versioned: Option<VersionedArguments>,
    recursive: Option<RecursiveArguments>,
    data_version: Flag,
    #[darling(default)]
    unique: PathList,
    #[darling(default)]
    states: HashMap<Ident, Expr>,
}

impl ToTokens for DaoArguments {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let mut args = Vec::new();
        if self.update.is_present() {
            args.push(quote! { update });
        }
        if let Some(sql_table) = &self.sql_table {
            args.push(quote! { sql_table = #sql_table });
        }
        if let Some(order_by) = &self.order_by {
            let OrderByArguments { col, dir } = order_by;
            args.push(quote! { order_by(col = #col, dir = #dir) });
        }
        if let Some(versioned) = &self.versioned {
            let mut v_args = Vec::new();
            if let Some(pk) = &versioned.pk {
                v_args.push(quote! { pk = #pk });
            }
            if let Some(version_by) = &versioned.version_by {
                v_args.push(quote! { version_by = #version_by });
            }
            if let Some(partition_by) = &versioned.partition_by {
                v_args.push(quote! { partition_by = #partition_by });
            }
            if let Some(current) = &versioned.current {
                v_args.push(quote! { current = #current });
            }
            args.push(quote! { versioned(#(#v_args),*) });
        }
        if let Some(recursive) = &self.recursive {
            let up = &recursive.up;
            let down = &recursive.down;
            args.push(quote! { recursive(up = #up, down = #down) });
        }
        if self.data_version.is_present() {
            args.push(quote! { data_version });
        }
        if !self.unique.is_empty() {
            let paths = self.unique.iter();
            args.push(quote! { unique(#(#paths),*) });
        }
        let states = self
            .states
            .iter()
            .map(|(name, value)| {
                quote! { #name = #value }
            })
            .collect::<Vec<_>>();
        if !states.is_empty() {
            args.push(quote! { states(#(#states),*) });
        }

        tokens.extend(quote! {
            #[dao(#(#args,)*)]
        });
    }
}

impl DaoArguments {
    pub fn override_with(self, other: DaoArguments) -> DaoArguments {
        DaoArguments {
            update: if other.update.is_present() {
                other.update
            } else {
                self.update
            },
            sql_table: other.sql_table.or(self.sql_table),
            order_by: other.order_by.or(self.order_by),
            versioned: match (self.versioned, other.versioned) {
                (Some(base), Some(child)) => Some(base.override_with(child)),
                (base, child) => child.or(base),
            },
            recursive: other.recursive.or(self.recursive),
            data_version: if other.data_version.is_present() {
                other.data_version
            } else {
                self.data_version
            },
            unique: if other.unique.is_empty() {
                self.unique
            } else {
                other.unique
            },
            states: other.states.into_iter().chain(self.states).collect(),
        }
    }
}

#[derive(Debug, FromMeta)]
struct OrderByArguments {
    col: String,
    #[darling(default = "default_order_dir")]
    dir: String,
}

fn default_order_dir() -> String {
    "desc".into()
}

#[derive(Debug, FromMeta)]
struct VersionedArguments {
    pk: Option<String>,
    version_by: Option<String>,
    partition_by: Option<String>,
    current: Option<String>,
}

impl VersionedArguments {
    fn override_with(self, other: VersionedArguments) -> VersionedArguments {
        VersionedArguments {
            pk: other.pk.or(self.pk),
            version_by: other.version_by.or(self.version_by),
            partition_by: other.partition_by.or(self.partition_by),
            current: other.current.or(self.current),
        }
    }
}

#[derive(Debug, FromMeta)]
struct RecursiveArguments {
    up: String,
    down: String,
}

// ---- Helpers ---------------------------------------------------------------

/// Look up a field's type by name, returning a compile error if not found.
/// Use inside functions that return `TokenStream` — the `return` unwinds to the
/// enclosing function on error.
macro_rules! require_field {
    ($fields:expr, $name:expr, $context:expr, $span:expr) => {
        match field_type_by_name($fields, $name) {
            Some(ty) => ty,
            None => {
                return syn::Error::new_spanned(
                    $span,
                    format!(concat!($context, ": field '{}' not found in struct"), $name),
                )
                .to_compile_error()
                .into();
            }
        }
    };
}

// ---- Entry points ----------------------------------------------------------

/// Attribute macro that wraps a struct with DAO derives (DaoType, Builder, and
/// optionally FromRow).  When the struct carries `#[dao(update, …)]` the type
/// is update-only and `sqlx::FromRow` is skipped (never read from DB).
pub fn dao(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let is_update = is_dao_update_attr(&input);
    if is_update {
        quote! {
            #[derive(Debug, Clone, ta_objects::DaoType, derive_builder::Builder)]
            #[builder(try_setter, setter(into))]
            #input
        }
        .into()
    } else {
        quote! {
            #[derive(Debug, Clone, ta_objects::DaoType, derive_builder::Builder, sqlx::FromRow)]
            #[builder(try_setter, setter(into))]
            #input
        }
        .into()
    }
}

/// Check whether any `#[dao(...)]` attribute on the struct contains `update`.
fn is_dao_update_attr(input: &ItemStruct) -> bool {
    input.attrs.iter().any(|a| {
        if !a.path().is_ident("dao") {
            return false;
        }
        // Parse the token list inside #[dao(...)]; look for a bare `update` ident.
        a.parse_args_with(|tokens: syn::parse::ParseStream| {
            while !tokens.is_empty() {
                if tokens.peek(syn::Ident) {
                    let ident: Ident = tokens.parse()?;
                    if ident == "update" {
                        return Ok(true);
                    }
                } else {
                    // Skip non-ident tokens (commas, nested groups, etc.)
                    let _: proc_macro2::TokenTree = tokens.parse()?;
                }
            }
            Ok(false)
        })
        .unwrap_or(false)
    })
}

/// Derive macro that generates the Iden enum, DaoCore, TypedColumns, and trait
/// impls (Dao, VersionedDao, Recursive, States) for a DAO struct.
pub fn dao_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parsed = match DaoArguments::from_derive_input(&input) {
        Ok(p) => p,
        Err(e) => return e.write_errors().into(),
    };
    let item = match parse_input_item_struct(&input) {
        Ok(item) => item,
        Err(e) => return e.to_compile_error().into(),
    };

    let ident = &item.ident;
    let vis = &item.vis;
    let fields = &item.fields;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // snake_case field idents, excluding #[sqlx(skip)] fields.
    // These are used for struct field access: self.defined_on, self.id, etc.
    let field_names = field_idents(fields);

    // The Rust types of each field — independent of the field name.
    // AccessTokenId, AtTime, etc. Used only in TypedColumns for TypeId matching.
    let field_types = field_types(fields);

    // Iden variant idents derived from FIELD NAMES (not type names) via UpperCamelCase.
    // "id" → Id, "defined_on" → DefinedOn, regardless of whether the type is
    // AccessTokenId, AtTime, or anything else.
    let field_iden_variants: Vec<Ident> = field_names.iter().map(|f| iden_variant(f)).collect();

    // ---- sql_table ---------------------------------------------------------

    let sql_table = parsed
        .sql_table
        .unwrap_or_else(|| ident.to_string().to_lowercase());

    // ---- Iden enum ---------------------------------------------------------
    let table_iden = format_ident!("{}Iden", ident);
    let field_name_strs: Vec<String> = field_names.iter().map(|f| f.to_string()).collect();
    let iden_enum = quote! {
        #[derive(Debug, Clone, Copy, sea_query::Iden)]
        #vis enum #table_iden {
            #[iden = #sql_table]
            Table,
            #(#field_iden_variants,)*
        }
        impl std::str::FromStr for #table_iden {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    #sql_table => Ok(Self::Table),
                    #(#field_name_strs => Ok(Self::#field_iden_variants),)*
                    _ => Err(td_error::api_error!(
                        td_error::ApiError::InputError,
                        "Unknown column: {}",
                        s
                    )),
                }
            }
        }
    };

    // ---- order_by ----------------------------------------------------------
    let order_by = match parsed.order_by {
        Some(o) => o,
        None => {
            let first = match field_iden_variants.first() {
                Some(f) => f,
                None => {
                    return syn::Error::new_spanned(
                        ident,
                        "DAO struct must have at least one non-skipped field",
                    )
                    .to_compile_error()
                    .into();
                }
            };
            OrderByArguments {
                col: first.to_string(),
                dir: default_order_dir(),
            }
        }
    };
    // The order_by col is a field name (snake_case) — convert to Iden variant.
    let order_col_variant = iden_variant_from_str(&order_by.col);
    let order_dir = match order_by.dir.as_str() {
        "asc" => quote! { sea_query::Order::Asc },
        _ => quote! { sea_query::Order::Desc },
    };

    // ---- DaoCore (minimal) -------------------------------------------------
    let display_name = struct_name_to_display_name(&ident.to_string());
    let dao_core = quote! {
        impl #impl_generics ta_objects::DaoCore for #ident #ty_generics #where_clause {
            fn table() -> Self::Iden {
                #table_iden::Table
            }
            fn display_name() -> &'static str {
                #display_name
            }
            fn column_display_values(&self) -> Vec<(String, String)> {
                use ta_objects::SeaValue;
                vec![
                    #(
                        (
                            sea_query::Iden::to_string(&#table_iden::#field_iden_variants),
                            self.#field_names.display_value(),
                        ),
                    )*
                ]
            }
        }
    };

    // ---- TypedColumns ------------------------------------------------------
    let typed_columns = quote! {
        impl #impl_generics ta_objects::TypedColumns for #ident #ty_generics #where_clause {
            type Iden = #table_iden;
            fn col_for_type(
                type_id: std::any::TypeId,
            ) -> Option<Self::Iden> {
                match type_id {
                    #(
                        t if t == std::any::TypeId::of::<#field_types>() =>
                            Some(#table_iden::#field_iden_variants),
                    )*
                    _ => None,
                }
            }
        }
    };

    // ---- DaoUpdate (update_values) -----------------------------------------
    //
    // For `Option<T>` fields (non-immutable): skip them when `None` so that
    // partial-update DTOs don't SET columns to NULL.
    let update_stmts: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .filter(|f| !is_sqlx_skip(f))
        .map(|f| {
            let name = f.ident.as_ref().unwrap();
            let variant = iden_variant(name);
            if is_dao_immutable(f) {
                // CASE WHEN col IS NULL THEN ? ELSE col END — set-once semantics.
                quote! {
                    __values.push((
                        #table_iden::#variant,
                        sea_query::Expr::case(
                            sea_query::Expr::col(#table_iden::#variant).is_null(),
                            self.#name.as_sea_value()
                        )
                        .finally(sea_query::Expr::col(#table_iden::#variant))
                        .into()
                    ));
                }
            } else if is_option_type(f) {
                // Only include the column when the value is Some.
                quote! {
                    if let Some(ref __v) = self.#name {
                        __values.push((
                            #table_iden::#variant,
                            __v.as_sea_value().into()
                        ));
                    }
                }
            } else {
                quote! {
                    __values.push((
                        #table_iden::#variant,
                        self.#name.as_sea_value().into()
                    ));
                }
            }
        })
        .collect();

    let dao_update_impl = quote! {
        impl #impl_generics ta_objects::DaoUpdate
            for #ident #ty_generics #where_clause {
                fn update_values(&self) -> impl IntoIterator<Item = (Self::Iden, sea_query::Expr)>
                {
                    use ta_objects::SeaValue;
                    use sea_query::ExprTrait;
                    let mut __values = Vec::new();
                    #(#update_stmts)*
                    __values
                }
        }
    };

    // ---- Update-only path: skip Dao, VersionedDao, States, Recursive -------
    if parsed.update.is_present() && parsed.versioned.is_some() {
        return syn::Error::new_spanned(
            ident,
            "`update` and `versioned` are incompatible — update DAOs cannot be versioned",
        )
        .to_compile_error()
        .into();
    }
    if parsed.update.is_present() {
        let td_type = td_type(&input, &item);
        return quote! {
            #iden_enum
            #typed_columns
            #dao_core
            #dao_update_impl
            #td_type
        }
        .into();
    }

    // ---- Dao (full read/write: columns, insert_values, order_by) -----------
    let dao_read = quote! {
        impl #impl_generics ta_objects::Dao for #ident #ty_generics #where_clause {
            fn columns() -> impl IntoIterator<Item = Self::Iden>
            {
                [#(#table_iden::#field_iden_variants),*]
            }
            fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr>
            {
                use ta_objects::SeaValue;
                [#(self.#field_names.as_sea_value().into()),*]
            }
            fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)>
            {
                [(#table_iden::#order_col_variant, #order_dir)]
            }
        }
    };

    // ---- Access trait: DaoUpdate, VersionedDao, or both --------------------
    let access_trait = match (&parsed.versioned, parsed.data_version.is_present()) {
        (None, false) => dao_update_impl,
        (Some(v), is_data_version) => {
            let pk_str = match &v.pk {
                Some(s) => s,
                None => {
                    return syn::Error::new_spanned(ident, "versioned: `pk` is required")
                        .to_compile_error()
                        .into();
                }
            };
            let version_by_str = match &v.version_by {
                Some(s) => s,
                None => {
                    return syn::Error::new_spanned(ident, "versioned: `version_by` is required")
                        .to_compile_error()
                        .into();
                }
            };
            let partition_by_str = match &v.partition_by {
                Some(s) => s,
                None => {
                    return syn::Error::new_spanned(ident, "versioned: `partition_by` is required")
                        .to_compile_error()
                        .into();
                }
            };
            let version_variant = iden_variant_from_str(version_by_str);
            let partition_variant = iden_variant_from_str(partition_by_str);
            let pk_variant = iden_variant_from_str(pk_str);
            let pk_type = require_field!(fields, pk_str, "versioned", ident);
            let version_type = require_field!(fields, version_by_str, "versioned", ident);
            let partition_type = require_field!(fields, partition_by_str, "versioned", ident);

            let current_impl = v.current.as_ref().map(|current_table| {
                quote! {
                    impl #impl_generics ta_objects::CurrentDao
                        for #ident #ty_generics #where_clause
                    {
                        fn current_table_name() -> &'static str {
                            #current_table
                        }
                    }
                }
            });

            let versioned_impl = quote! {
                impl #impl_generics ta_objects::VersionedDao
                    for #ident #ty_generics #where_clause
                {
                    type Pk = #pk_type;
                    fn pk_col() -> Self::Iden {
                        #table_iden::#pk_variant
                    }

                    type Partition = #partition_type;
                    fn partition_col() -> Self::Iden {
                        #table_iden::#partition_variant
                    }

                    type VersionOrder = #version_type;
                    fn version_order_col() -> Self::Iden {
                        #table_iden::#version_variant
                    }
                }
                #current_impl
            };

            if is_data_version {
                let data_version_impl = quote! {
                    impl #impl_generics ta_objects::DataVersionDao
                        for #ident #ty_generics #where_clause {}
                };
                quote! { #versioned_impl #dao_update_impl #data_version_impl }
            } else {
                versioned_impl
            }
        }
        (None, true) => {
            return syn::Error::new_spanned(
                ident,
                "data_version requires versioned(...) to be set",
            )
            .to_compile_error()
            .into();
        }
    };

    // ---- States ----------------------------------------------------------------
    let states = if parsed.versioned.is_some() || !parsed.states.is_empty() {
        let all_state = quote! {
            impl #impl_generics #ident #ty_generics #where_clause {
                #[allow(non_upper_case_globals)]
                pub const All: u8 = 0;
            }
            #[allow(clippy::unused_unit)]
            impl #impl_generics ta_objects::States<0> for #ident #ty_generics #where_clause {
                fn state() -> impl ta_objects::sql_condition::SqlCondition {
                    ()
                }
            }
        };
        // Sort states by name to ensure deterministic const numbering across compilations.
        let mut sorted_states: Vec<_> = parsed.states.iter().collect();
        sorted_states.sort_by_key(|(name, _)| name.to_string());
        let user_states = sorted_states
            .into_iter()
            .enumerate()
            .map(|(i, (name, value))| {
                let i = i as u8 + 1;
                quote! {
                    impl #impl_generics #ident #ty_generics #where_clause {
                        #[allow(non_upper_case_globals)]
                        pub const #name: u8 = #i;
                    }
                    impl #impl_generics ta_objects::States<#i>
                        for #ident #ty_generics #where_clause
                    {
                        fn state() -> impl ta_objects::sql_condition::SqlCondition {
                            #value
                        }
                    }
                }
            });
        quote! { #all_state #(#user_states)* }
    } else {
        quote! {}
    };

    // ---- Recursive ---------------------------------------------------------
    let recursive = match parsed.recursive {
        None => quote! {},
        Some(r) => {
            let up_type = require_field!(fields, &r.up, "recursive", ident);
            let down_type = require_field!(fields, &r.down, "recursive", ident);
            if up_type != down_type {
                return syn::Error::new_spanned(
                    ident,
                    "Recursive: up and down columns must have the same type",
                )
                .to_compile_error()
                .into();
            }
            let up_variant = iden_variant_from_str(&r.up);
            let down_variant = iden_variant_from_str(&r.down);
            quote! {
                impl #impl_generics ta_objects::Recursive
                    for #ident #ty_generics #where_clause
                {
                    type Key = #up_type;
                    fn recurse_up() -> Self::Iden {
                        #table_iden::#up_variant
                    }
                    fn recurse_down() -> Self::Iden {
                        #table_iden::#down_variant
                    }
                }
            }
        }
    };

    let td_type = td_type(&input, &item);

    quote! {
        #iden_enum
        #typed_columns
        #dao_core
        #dao_read
        #access_trait
        #states
        #recursive
        #td_type
    }
    .into()
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert a snake_case field name string to the UpperCamelCase Iden variant
/// ident that sea_query::enum_def generates.
pub fn iden_variant_from_str(field_name: &str) -> Ident {
    format_ident!("{}", field_name.to_upper_camel_case())
}

/// Same but from an existing Ident.
pub fn iden_variant(field_ident: &Ident) -> Ident {
    iden_variant_from_str(&field_ident.to_string())
}

/// All non-skipped field idents (snake_case, as written in the struct).
pub fn field_idents(fields: &Fields) -> Vec<&Ident> {
    fields
        .iter()
        .filter(|f| !is_sqlx_skip(f))
        .filter_map(|f| f.ident.as_ref())
        .collect()
}

/// All non-skipped field types.
pub fn field_types(fields: &Fields) -> Vec<&Type> {
    fields
        .iter()
        .filter(|f| !is_sqlx_skip(f))
        .map(|f| &f.ty)
        .collect()
}

/// Resolve the type of a named field, unwrapping `Option<T>` → `T`.
/// NULL comparisons in SQL are always false, so Option and T are equivalent
/// for column matching purposes.
/// Returns `None` if the field is not found.
pub fn field_type_by_name<'a>(fields: &'a Fields, name: &str) -> Option<&'a Type> {
    let ty = fields
        .iter()
        .find_map(|f| f.ident.as_ref().filter(|i| *i == name).map(|_| &f.ty))?;

    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }
    Some(ty)
}

fn is_sqlx_skip(f: &syn::Field) -> bool {
    f.attrs.iter().any(|a| {
        if !a.path().is_ident("sqlx") {
            return false;
        }
        // Parse #[sqlx(skip)] — the inner token is just the ident "skip"
        a.parse_args::<Ident>()
            .map(|ident| ident == "skip")
            .unwrap_or(false)
    })
}

/// Convert a DAO struct name to a human-readable display name.
/// Strips from the first `DB` occurrence onwards, then inserts spaces
/// before uppercase-to-lowercase transitions (CamelCase word boundaries).
/// `UserDB` → `"User"`, `FunctionRunDB` → `"Function Run"`,
/// `FunctionDBWithNames` → `"Function"`.
fn struct_name_to_display_name(name: &str) -> String {
    let name = match name.find("DB") {
        Some(pos) if pos > 0 => &name[..pos],
        _ => name,
    };
    let mut result = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && c.is_uppercase() {
            // Only add a space at a real word boundary:
            // uppercase followed by lowercase (e.g. "Run" in "FunctionRun")
            // or preceded by lowercase (e.g. "R" after "function")
            let prev_lower = chars[i - 1].is_lowercase();
            let next_lower = chars.get(i + 1).is_some_and(|c| c.is_lowercase());
            if prev_lower || next_lower {
                result.push(' ');
            }
        }
        result.push(c);
    }
    result
}

fn is_dao_immutable(f: &syn::Field) -> bool {
    f.attrs.iter().any(|a| {
        if !a.path().is_ident("dao") {
            return false;
        }
        a.parse_args::<Ident>()
            .map(|ident| ident == "immutable")
            .unwrap_or(false)
    })
}

/// Returns `true` when the field type is `Option<…>`.
fn is_option_type(f: &syn::Field) -> bool {
    if let Type::Path(tp) = &f.ty
        && let Some(seg) = tp.path.segments.last()
    {
        return seg.ident == "Option";
    }
    false
}
