//
// Copyright 2025 Tabsdata Inc.
//

// Iden variants derive from field names via heck's UpperCamelCase
// (`defined_on` → `DefinedOn`); every macro-side column reference must use
// the same conversion.

use crate::dao_marks::{FieldMarks, field_ident, field_type, option_inner_type, type_names};
use crate::dao_unique::{Under, addresses};
use crate::dxo_fields::{callback_ident, fields_callback};
use crate::type_builder::parse_input_item_struct;
use darling::ast::NestedMeta;
use darling::util::{Flag, PathList};
use darling::{FromDeriveInput, FromMeta};
use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{DeriveInput, Field, Fields, ItemStruct, Type, parse_macro_input};

/// `#[dao(table = "..", ops(select, insert, update(RowDao), delete, upsert(A, B)))]`
/// or `#[dao(view = "..")]`, plus `db(Domain)`, `unique(A, B)` (repeatable;
/// unique across the whole relation, index-backed), `unique_at(A, B)`
/// (repeatable, `scd2` only; unique among the versions visible at one instant,
/// so it addresses one version but not one row of the history), `scd2` |
/// `timeline`, `recursive`, and `project_scoped` /
/// `project_scoped(unique(A, B), unique_at(A, B))`.
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(dao))]
pub struct DaoArguments {
    ops: Option<OpsArguments>,
    table: Option<String>,
    view: Option<String>,
    db: Option<PathArg>,
    #[darling(default, multiple, rename = "unique")]
    unique_types: Vec<PathList>,
    #[darling(default, multiple, rename = "unique_at")]
    unique_at_types: Vec<PathList>,
    scd2: Flag,
    timeline: Flag,
    recursive: Flag,
    project_scoped: Option<ProjectScopedArguments>,
}

#[derive(Debug, Default, FromMeta)]
struct OpsArguments {
    select: Flag,
    insert: Flag,
    delete: Flag,
    update: Option<UpdateArguments>,
    upsert: Option<PathList>,
}

impl OpsArguments {
    fn writes(&self) -> bool {
        self.insert.is_present() || self.delete.is_present() || self.upsert.is_some()
    }

    fn any(&self) -> bool {
        self.select.is_present() || self.writes() || self.update.is_some()
    }
}

#[derive(Debug, Default)]
struct ProjectScopedArguments {
    unique: Vec<PathList>,
    unique_at: Vec<PathList>,
}

impl FromMeta for ProjectScopedArguments {
    fn from_word() -> darling::Result<Self> {
        Ok(Self::default())
    }

    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        let mut unique = Vec::new();
        let mut unique_at = Vec::new();
        for item in items {
            match item {
                NestedMeta::Meta(meta) if meta.path().is_ident("unique") => {
                    unique.push(PathList::from_meta(meta)?);
                }
                NestedMeta::Meta(meta) if meta.path().is_ident("unique_at") => {
                    unique_at.push(PathList::from_meta(meta)?);
                }
                other => {
                    return Err(darling::Error::custom(
                        "project_scoped takes `unique(A, B)` / `unique_at(A, B)` sets, as in \
                         `project_scoped(unique(CollectionName))`",
                    )
                    .with_span(other));
                }
            }
        }
        Ok(Self { unique, unique_at })
    }
}

#[derive(Debug)]
struct PathArg(syn::Path);

impl FromMeta for PathArg {
    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        single_path(items, "db(SomeDomain)").map(Self)
    }
}

#[derive(Debug, Default)]
struct UpdateArguments {
    filter_by: Option<syn::Path>,
}

impl FromMeta for UpdateArguments {
    fn from_word() -> darling::Result<Self> {
        Ok(Self::default())
    }

    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        single_path(items, "update(RowDao)").map(|p| Self { filter_by: Some(p) })
    }
}

fn single_path(items: &[NestedMeta], form: &str) -> darling::Result<syn::Path> {
    match items {
        [NestedMeta::Meta(syn::Meta::Path(path))] => Ok(path.clone()),
        _ => Err(darling::Error::custom(format!(
            "expected one type in parentheses, as in `{form}`"
        ))),
    }
}

/// An update payload's `Option` fields mean "leave the column alone", so it is
/// the one shape that gets no `sqlx::FromRow`.
pub fn dao(_args: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as ItemStruct);
    let parsed = match DaoArguments::from_derive_input(&DeriveInput::from(input.clone())) {
        Ok(p) => p,
        Err(e) => return e.write_errors().into(),
    };
    let marks = match FieldMarks::parse(&input.fields) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error().into(),
    };
    let flattened: Vec<usize> = input
        .fields
        .iter()
        .enumerate()
        .filter(|(_, f)| marks.has(f, "flatten"))
        .map(|(i, _)| i)
        .collect();
    if let Fields::Named(named) = &mut input.fields {
        for (i, f) in named.named.iter_mut().enumerate() {
            if flattened.contains(&i) {
                f.attrs.push(syn::parse_quote! { #[sqlx(flatten)] });
            }
        }
    }
    let from_row = match parsed.ops.as_ref().and_then(|ops| ops.update.as_ref()) {
        Some(_) => quote! {},
        None => quote! { , sqlx::FromRow },
    };
    quote! {
        #[derive(Debug, Clone, ta_objects::DaoType #from_row)]
        #[allow(clippy::duplicated_attributes)]
        #input
    }
    .into()
}

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
    match expand(&parsed, &item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct Column<'a> {
    name: &'a Ident,
    ty: &'a Type,
    variant: Ident,
}

fn expand(parsed: &DaoArguments, item: &ItemStruct) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &item.ident;
    let vis = &item.vis;
    let fields = &item.fields;
    if !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "a DAO cannot be generic: it maps to one relation",
        ));
    }
    let marks = FieldMarks::parse(fields)?;
    let flatten = flatten_info(&marks)?;
    let base_ty = flatten.as_ref().map(|fi| &fi.base_ty);
    let interval = fields.iter().find(|f| type_names(&f.ty, "Scd2Interval"));
    if let Some(f) = interval
        && !marks.has(f, "flatten")
    {
        return Err(syn::Error::new_spanned(
            f,
            "the interval is a flattened projection: mark it #[dao(flatten)]",
        ));
    }
    let columns: Vec<Column> = fields
        .iter()
        .filter(|f| !is_sqlx_skip(f) && !marks.has(f, "flatten"))
        .map(|f| Column {
            name: field_ident(f),
            ty: &f.ty,
            variant: iden_variant(field_ident(f)),
        })
        .collect();
    let field_names: Vec<&Ident> = columns.iter().map(|c| c.name).collect();
    let field_types: Vec<&Type> = columns.iter().map(|c| c.ty).collect();
    let field_iden_variants: Vec<&Ident> = columns.iter().map(|c| &c.variant).collect();

    let (relation, is_table) = match (&parsed.table, &parsed.view) {
        (Some(t), None) => (t.clone(), true),
        (None, Some(v)) => (v.clone(), false),
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "a DAO must declare exactly one of `table = \"...\"` / `view = \"...\"`",
            ));
        }
    };
    let view_ops = OpsArguments {
        select: Flag::present(),
        ..Default::default()
    };
    let ops = match (&parsed.ops, is_table) {
        (Some(ops), true) if ops.any() => ops,
        (None, false) => &view_ops,
        (_, true) => {
            return Err(syn::Error::new_spanned(
                ident,
                "a table DAO must declare what it may be the subject of, \
                 as in `ops(select, insert)`",
            ));
        }
        (Some(_), false) => {
            return Err(syn::Error::new_spanned(
                ident,
                "a view is readable by construction and cannot be written to, so it takes no `ops`",
            ));
        }
    };
    let is_scd2 = parsed.scd2.is_present();
    let is_timeline = parsed.timeline.is_present();
    let is_versioned = is_scd2 || is_timeline;
    let is_update = ops.update.is_some();
    if is_update && (ops.select.is_present() || ops.writes()) {
        return Err(syn::Error::new_spanned(
            ident,
            "`update` is the only op of an update payload: its `Option` fields mean \
             \"leave the column alone\", which a readable or insertable DAO reads as \
             \"the column is nullable\"",
        ));
    }
    if ops.upsert.is_some() && !ops.insert.is_present() {
        return Err(syn::Error::new_spanned(
            ident,
            "`upsert` is a conflicting `insert`, so it needs one",
        ));
    }
    if is_scd2 && ops.insert.is_present() && ops.select.is_present() {
        return Err(syn::Error::new_spanned(
            ident,
            "an `scd2` relation has a payload (`ops(insert)`) and a row DAO (`ops(select)`) \
             projecting the interval the doors wrote; one struct cannot be both",
        ));
    }
    if is_scd2 && is_timeline {
        return Err(syn::Error::new_spanned(
            ident,
            "`scd2` and `timeline` are mutually exclusive",
        ));
    }
    if is_versioned && ops.upsert.is_some() {
        return Err(syn::Error::new_spanned(
            ident,
            "a versioned row is superseded, never conflict-replaced: `upsert` needs an \
             unversioned relation",
        ));
    }
    if is_versioned && ops.delete.is_present() {
        return Err(syn::Error::new_spanned(
            ident,
            "a versioned row is closed by the trigger, never deleted: `delete` needs an \
             unversioned relation",
        ));
    }
    let declared: Vec<&str> = [
        is_scd2.then_some("scd2"),
        is_timeline.then_some("timeline"),
        parsed.recursive.is_present().then_some("recursive"),
    ]
    .into_iter()
    .flatten()
    .collect();
    marks.check_grains(&declared)?;
    if !is_versioned && marks.marked_once("partition_by")?.is_some() {
        return Err(syn::Error::new_spanned(
            ident,
            "#[dao(partition_by)] requires `scd2` or `timeline`",
        ));
    }

    let table_iden = format_ident!("{}Iden", ident);
    let col_ref = |field: &Field| -> proc_macro2::TokenStream {
        let variant = iden_variant(field_ident(field));
        quote! { #table_iden::#variant }
    };

    if let Some(f) = interval
        && !is_scd2
    {
        return Err(syn::Error::new_spanned(
            f,
            "`Scd2Interval` is the interval an `scd2` row DAO projects",
        ));
    }
    if ops.insert.is_present() {
        if let Some(f) = interval {
            return Err(syn::Error::new_spanned(
                f,
                "a payload never carries the interval: `open(.., at)` binds `valid_from` and \
                 the close-out trigger `valid_to`. Project it from the row DAO, which declares \
                 `select` only",
            ));
        }
        if let Some(c) = columns.iter().find(|c| {
            type_names(c.ty, "Scd2ValidTo") || c.name == "valid_from" || c.name == "valid_to"
        }) {
            return Err(syn::Error::new_spanned(
                c.name,
                "a payload never carries the interval columns: `open(.., at)` binds \
                 `valid_from` and the close-out trigger `valid_to`",
            ));
        }
    }
    if is_scd2 && !ops.insert.is_present() && interval.is_none() && base_ty.is_none() {
        return Err(syn::Error::new_spanned(
            ident,
            "an `scd2` row DAO projects its interval: declare `pub interval: Scd2Interval`",
        ));
    }
    if parsed.project_scoped.is_some()
        && ops.insert.is_present()
        && let Some(c) = columns.iter().find(|c| type_names(c.ty, "ProjectId"))
    {
        return Err(syn::Error::new_spanned(
            c.name,
            "a `project_scoped` insert DAO cannot carry a `ProjectId` column: the \
             `project_isolation` policy defaults it from the connection. Keep it \
             on the row DAO, which declares `select` only",
        ));
    }

    let own_interval_idens = is_scd2 && (interval.is_some() || base_ty.is_none());
    let (valid_from_col, valid_to_col) = match (own_interval_idens, base_ty) {
        (true, _) | (false, None) => (
            quote! { #table_iden::ValidFrom },
            quote! { #table_iden::ValidTo },
        ),
        (false, Some(b)) => (
            quote! { #table_iden::Base(<#b as ta_objects::Scd2Dao>::VALID_FROM) },
            quote! { #table_iden::Base(<#b as ta_objects::Scd2Dao>::VALID_TO) },
        ),
    };
    let synthetic_variant = own_interval_idens.then(|| quote! { ValidFrom, ValidTo, });
    let synthetic_from_str = own_interval_idens.then(|| {
        quote! {
            "valid_from" => Ok(Self::ValidFrom),
            "valid_to" => Ok(Self::ValidTo),
        }
    });
    let field_name_strs: Vec<String> = field_names.iter().map(|f| f.to_string()).collect();
    let iden_enum = match &flatten {
        None => quote! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, sea_query::Iden)]
            #vis enum #table_iden {
                #[iden = #relation]
                Table,
                #synthetic_variant
                #(#field_iden_variants,)*
            }
            impl std::str::FromStr for #table_iden {
                type Err = td_error::TdError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    match s {
                        #relation => Ok(Self::Table),
                        #synthetic_from_str
                        #(#field_name_strs => Ok(Self::#field_iden_variants),)*
                        _ => Err(td_error::api_error!(
                            td_error::ApiError::InputError,
                            "Unknown column: {}",
                            s
                        )),
                    }
                }
            }
        },
        Some(fi) => {
            let base_iden = &fi.base_iden;
            quote! {
                #[derive(Debug, Clone, Copy, PartialEq, Eq, sea_query::Iden)]
                #vis enum #table_iden {
                    #[iden = #relation]
                    Table,
                    #[iden(flatten)]
                    Base(#base_iden),
                    #synthetic_variant
                    #(#field_iden_variants,)*
                }
                impl std::str::FromStr for #table_iden {
                    type Err = td_error::TdError;
                    fn from_str(s: &str) -> Result<Self, Self::Err> {
                        match s {
                            #relation => Ok(Self::Table),
                            #synthetic_from_str
                            #(#field_name_strs => Ok(Self::#field_iden_variants),)*
                            _ => <#base_iden as std::str::FromStr>::from_str(s).map(Self::Base),
                        }
                    }
                }
            }
        }
    };

    let order_by_items = match marks.order_by()? {
        Some((name, asc)) => {
            let variant = match interval {
                Some(f) if field_ident(f) == name => format_ident!("ValidFrom"),
                _ => iden_variant(name),
            };
            let dir = if asc {
                quote! { sea_query::Order::Asc }
            } else {
                quote! { sea_query::Order::Desc }
            };
            quote! { [(#table_iden::#variant, #dir)] }
        }
        None => match base_ty {
            Some(b) => quote! {
                <#b as ta_objects::Dao>::order_by()
                    .into_iter()
                    .map(|(c, o)| (#table_iden::Base(c), o))
            },
            None => {
                let first = field_iden_variants.first().ok_or_else(|| {
                    syn::Error::new_spanned(ident, "a DAO needs at least one column")
                })?;
                quote! { [(#table_iden::#first, sea_query::Order::Desc)] }
            }
        },
    };

    let display_name = struct_name_to_display_name(&ident.to_string());
    let db_domain = match &parsed.db {
        Some(PathArg(path)) => quote! { #path },
        None => quote! { ta_objects::Apiserver },
    };
    let interval_columns = interval
        .map(|_| quote! { .into_iter().chain([#table_iden::ValidFrom, #table_iden::ValidTo]) });
    let interval_values = interval.map(|f| {
        let name = field_ident(f);
        quote! {
            __values.extend([
                ("valid_from".to_string(), self.#name.valid_from.display_value()),
                ("valid_to".to_string(), self.#name.valid_to.display_value()),
            ]);
        }
    });
    let (display_values_base, columns_items, insert_items) = match &flatten {
        None => (
            quote! { let mut __values: Vec<(String, String)> = Vec::new(); },
            quote! { [#(#table_iden::#field_iden_variants),*] #interval_columns },
            quote! { [#(self.#field_names.as_sea_value().into()),*] },
        ),
        Some(fi) => {
            let b = &fi.base_ty;
            let bf = &fi.field;
            (
                quote! { let mut __values = self.#bf.column_display_values(); },
                quote! {
                    <#b as ta_objects::Dao>::columns()
                        .into_iter()
                        .map(#table_iden::Base)
                        .chain([#(#table_iden::#field_iden_variants),*])
                        #interval_columns
                },
                quote! {
                    ta_objects::Insertable::insert_values(&self.#bf)
                        .into_iter()
                        .chain([#(self.#field_names.as_sea_value().into()),*])
                },
            )
        }
    };
    let dao_core = quote! {
        impl ta_objects::Dao for #ident {
            type Iden = #table_iden;
            type Db = #db_domain;
            const RELATION: &'static str = #relation;
            const TABLE: Self::Iden = #table_iden::Table;
            fn display_name() -> &'static str {
                #display_name
            }
            fn columns() -> impl IntoIterator<Item = Self::Iden> {
                #columns_items
            }
            fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)> {
                #order_by_items
            }
            fn column_display_values(&self) -> Vec<(String, String)> {
                #[allow(unused_imports)]
                use ta_objects::SeaValue;
                #display_values_base
                __values.extend([
                    #(
                        (
                            sea_query::Iden::to_string(&#table_iden::#field_iden_variants),
                            self.#field_names.display_value(),
                        ),
                    )*
                ]);
                #interval_values
                __values
            }
        }
    };

    let is_project_scoped = parsed.project_scoped.is_some();
    let reachable = if is_project_scoped {
        quote! {
            impl ta_objects::ProjectScoped for #ident {}
            impl<__S: td_database::access::SeesProjectRows> ta_objects::ReachableFrom<__S> for #ident {}
        }
    } else {
        quote! {
            impl<__S: td_database::access::DbScope> ta_objects::ReachableFrom<__S> for #ident {}
        }
    };
    let insertable = match (ops.insert.is_present(), is_project_scoped) {
        (false, _) => quote! {},
        (true, true) => quote! {
            impl ta_objects::InsertableFrom<td_database::access::InProject> for #ident {}
        },
        (true, false) => quote! {
            impl<__S: td_database::access::DbScope> ta_objects::InsertableFrom<__S> for #ident {}
        },
    };
    let table_dao = is_table.then(|| quote! { impl ta_objects::TableDao for #ident {} });

    let interval_has_columns = interval.map(|f| {
        let ty = field_type(f);
        quote! {
            impl ta_objects::HasColumn<<#ty as ta_objects::IntervalColumns>::ValidFrom> for #ident {
                const COL: Self::Iden = #table_iden::ValidFrom;
            }
            impl ta_objects::HasColumn<<#ty as ta_objects::IntervalColumns>::ValidTo> for #ident {
                const COL: Self::Iden = #table_iden::ValidTo;
            }
        }
    });
    let interval_fields = interval.map(|f| {
        let ty = field_type(f);
        quote! {
            [] { valid_from : <#ty as ta_objects::IntervalColumns>::ValidFrom }
            [] { valid_to : <#ty as ta_objects::IntervalColumns>::ValidTo }
        }
    });
    let has_columns = match &flatten {
        None => {
            let mut seen: HashMap<String, &Ident> = HashMap::new();
            for c in &columns {
                let ty = c.ty;
                if let Some(first) = seen.insert(quote!(#ty).to_string(), c.name) {
                    return Err(syn::Error::new_spanned(
                        c.name,
                        format!(
                            "duplicate column type `{}` on fields `{first}` and `{}`: give \
                             each column a distinct newtype",
                            quote!(#ty),
                            c.name
                        ),
                    ));
                }
            }
            let mut inner_seen: HashMap<String, &Ident> = HashMap::new();
            let nullable: Vec<(&Type, &Ident)> = columns
                .iter()
                .filter_map(|c| {
                    let inner = option_inner_type(c.ty)?;
                    let key = quote!(#inner).to_string();
                    if seen.contains_key(&key) || inner_seen.insert(key, c.name).is_some() {
                        return None;
                    }
                    Some((inner, &c.variant))
                })
                .collect();
            let nullable_tys = nullable.iter().map(|(t, _)| *t);
            let nullable_variants = nullable.iter().map(|(_, v)| *v);
            quote! {
                #(
                    impl ta_objects::HasColumn<#field_types> for #ident {
                        const COL: Self::Iden = #table_iden::#field_iden_variants;
                    }
                )*
                #(
                    impl ta_objects::HasColumn<#nullable_tys> for #ident {
                        const COL: Self::Iden = #table_iden::#nullable_variants;
                    }
                )*
                #interval_has_columns
            }
        }
        Some(fi) => {
            let base_cb = callback_ident(&fi.base_last_ident());
            let base_iden = &fi.base_iden;
            let bans_project_id = is_project_scoped && ops.insert.is_present();
            let bans_valid_to = ops.insert.is_present();
            quote! {
                #base_cb! {
                    ta_objects::__dao_has_columns => { #ident, #table_iden, #bans_project_id, #bans_valid_to }
                    [#base_iden]
                    #( [] { #field_names : #field_types } )*
                    #interval_fields
                }
            }
        }
    };

    let interval_pairs: Vec<(Ident, Type)> = interval
        .map(|f| {
            let ty = field_type(f);
            vec![
                (
                    format_ident!("valid_from"),
                    syn::parse_quote! { <#ty as ta_objects::IntervalColumns>::ValidFrom },
                ),
                (
                    format_ident!("valid_to"),
                    syn::parse_quote! { <#ty as ta_objects::IntervalColumns>::ValidTo },
                ),
            ]
        })
        .unwrap_or_default();
    let pairs: Vec<(&Ident, &Type)> = columns
        .iter()
        .map(|c| (c.name, c.ty))
        .chain(interval_pairs.iter().map(|(n, t)| (n, t)))
        .collect();
    let fields_cb = fields_callback(
        ident,
        &pairs,
        flatten
            .as_ref()
            .map(|fi| (callback_ident(&fi.base_last_ident()), &fi.base_iden)),
    );

    let update_impl = match &ops.update {
        None => quote! {},
        Some(update) => {
            let stmts = fields
                .iter()
                .filter(|f| !is_sqlx_skip(f))
                .map(|f| update_statement(f, &marks, &table_iden));
            let (filter_by, assert) = match &update.filter_by {
                None => (quote! { Self }, quote! {}),
                Some(path) => (
                    quote! { #path },
                    quote! {
                        const _: () = assert!(
                            ta_objects::relations_match(
                                <#ident as ta_objects::Dao>::RELATION,
                                <#path as ta_objects::Dao>::RELATION,
                            ),
                            concat!(
                                "update(...) must name a DAO over the same relation as ",
                                stringify!(#ident),
                            ),
                        );
                    },
                ),
            };
            quote! {
                #assert
                impl ta_objects::UpdateDao for #ident {
                    type FilterBy = #filter_by;
                    fn update_values(&self) -> impl IntoIterator<Item = (Self::Iden, sea_query::Expr)> {
                        #[allow(unused_imports)]
                        use ta_objects::SeaValue;
                        use sea_query::ExprTrait;
                        let mut __values = Vec::new();
                        #(#stmts)*
                        __values
                    }
                }
            }
        }
    };

    let select_impl = ops
        .select
        .is_present()
        .then(|| quote! { impl ta_objects::SelectDao for #ident {} });
    let insert_impl = ops.insert.is_present().then(|| {
        let capability = if is_scd2 {
            quote! { impl ta_objects::Scd2OpenDao for #ident {} }
        } else {
            quote! { impl ta_objects::InsertDao for #ident {} }
        };
        quote! {
            impl ta_objects::Insertable for #ident {
                fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr> {
                    #[allow(unused_imports)]
                    use ta_objects::SeaValue;
                    #insert_items
                }
            }
            #capability
        }
    });
    let delete_impl = ops
        .delete
        .is_present()
        .then(|| quote! { impl ta_objects::DeleteDao for #ident {} });
    let unversioned_impl = (!is_versioned).then(|| {
        let base_agrees = base_ty.map(|b| {
            quote! {
                const _: () = {
                    fn base_must_be_unversioned<T: ta_objects::UnversionedDao>() {}
                    let _ = base_must_be_unversioned::<#b>;
                };
            }
        });
        quote! {
            #base_agrees
            impl ta_objects::UnversionedDao for #ident {}
        }
    });
    let upsert_impl = ops.upsert.as_ref().map(|types| {
        let key_cols = types
            .iter()
            .map(|t| quote! { <Self as ta_objects::HasColumn<#t>>::COL });
        quote! {
            impl ta_objects::UpsertDao for #ident {
                fn key_columns() -> impl IntoIterator<Item = Self::Iden> {
                    [#(#key_cols),*]
                }
                fn value_columns() -> impl IntoIterator<Item = Self::Iden> {
                    let __keys: Vec<_> =
                        <Self as ta_objects::UpsertDao>::key_columns().into_iter().collect();
                    <Self as ta_objects::Dao>::columns()
                        .into_iter()
                        .filter(move |c| !__keys.contains(c))
                }
            }
        }
    });

    let role = |mark: &str, base_const: &str, base_assoc: &str, base_trait: &str| {
        let base_const = format_ident!("{}", base_const);
        let base_assoc = format_ident!("{}", base_assoc);
        let base_trait = format_ident!("{}", base_trait);
        marks.marked_once(mark).map(|field| match (field, base_ty) {
            (Some(f), _) => {
                let ty = field_type(f);
                Some((col_ref(f), quote! { #ty }))
            }
            (None, Some(b)) => Some((
                quote! { #table_iden::Base(<#b as ta_objects::#base_trait>::#base_const) },
                quote! { <#b as ta_objects::#base_trait>::#base_assoc },
            )),
            (None, None) => None,
        })
    };

    let pk = if is_update {
        None
    } else {
        role("pk", "PK", "Pk", "PkDao")?
    };
    let pk_impl = pk.as_ref().map(|(col, ty)| {
        quote! {
            impl ta_objects::PkDao for #ident {
                type Pk = #ty;
                const PK: Self::Iden = #col;
            }
        }
    });
    if is_versioned && pk.is_none() {
        return Err(syn::Error::new_spanned(
            ident,
            "scd2/timeline: mark the pk column with #[dao(pk)]",
        ));
    }

    let partition = role("partition_by", "PARTITION", "Partition", "Partitioned")?;
    let version_order = role("version_by", "VERSION_ORDER", "VersionOrder", "TimelineDao")?;
    let timeline_impl = is_versioned
        .then(|| {
            let kind = if is_scd2 { "scd2" } else { "timeline" };
            let (partition_col, partition_ty) = partition.clone().ok_or_else(|| {
                syn::Error::new_spanned(
                    ident,
                    format!("{kind}: mark the partition column with #[dao(partition_by)]"),
                )
            })?;
            let partitioned = quote! {
                impl ta_objects::Partitioned for #ident {
                    type Partition = #partition_ty;
                    const PARTITION: Self::Iden = #partition_col;
                }
            };
            let (version_col, version_ty) = if is_scd2 {
                let version_ty = match (interval, base_ty) {
                    (Some(f), _) => {
                        let ty = field_type(f);
                        quote! { <#ty as ta_objects::IntervalColumns>::ValidFrom }
                    }
                    (None, Some(b)) => quote! { <#b as ta_objects::TimelineDao>::VersionOrder },
                    (None, None) => quote! {},
                };
                (valid_from_col.clone(), version_ty)
            } else {
                version_order.clone().ok_or_else(|| {
                    syn::Error::new_spanned(
                        ident,
                        format!(
                            "{kind}: mark the version column with #[dao(timeline(version_by))]"
                        ),
                    )
                })?
            };
            let scd2_impl = is_scd2.then(|| {
                quote! {
                    impl ta_objects::Scd2Dao for #ident {
                        const VALID_FROM: Self::Iden = #valid_from_col;
                        const VALID_TO: Self::Iden = #valid_to_col;
                    }
                }
            });
            let readable = !ops.insert.is_present() || !is_scd2;
            let timeline = readable.then(|| {
                quote! {
                    impl ta_objects::TimelineDao for #ident {
                        type VersionOrder = #version_ty;
                        const VERSION_ORDER: Self::Iden = #version_col;
                    }
                }
            });
            Ok::<_, syn::Error>(quote! {
                #partitioned
                #scd2_impl
                #timeline
            })
        })
        .transpose()?;

    let addressing = if is_update {
        quote! {}
    } else {
        let as_types = |list: &PathList| -> Vec<Type> {
            list.iter().map(|p| syn::parse_quote! { #p }).collect()
        };
        let mut unique_sets: Vec<Vec<Type>> = parsed.unique_types.iter().map(as_types).collect();
        if let Some(types) = &ops.upsert {
            unique_sets.push(as_types(types));
        }
        let mut global_sets = unique_sets.clone();
        if let Some((_, ty)) = &pk {
            global_sets.push(vec![syn::parse_quote! { #ty }]);
        }
        let project_sets: Vec<Vec<Type>> = parsed
            .project_scoped
            .iter()
            .flat_map(|s| s.unique.iter().map(as_types))
            .collect();
        let at_sets: Vec<Vec<Type>> = parsed
            .unique_at_types
            .iter()
            .chain(
                parsed
                    .project_scoped
                    .iter()
                    .flat_map(|s| s.unique_at.iter()),
            )
            .map(as_types)
            .collect();
        if !at_sets.is_empty() && !is_scd2 {
            return Err(syn::Error::new_spanned(
                ident,
                "`unique_at(..)` is unique among the versions visible at one instant, \
                 which only an `scd2` relation has; an unversioned set is `unique(..)`",
            ));
        }
        let global_sets = minimal_sets(global_sets);
        let project_sets = minimal_sets(project_sets);
        let at_sets = minimal_sets(at_sets);
        let unique_sets = minimal_sets(unique_sets);
        let type_key = |t: &Type| quote!(#t).to_string();
        if let Some(shared) = global_sets.iter().flatten().find(|t| {
            project_sets
                .iter()
                .flatten()
                .any(|p| type_key(p) == type_key(t))
        }) {
            return Err(syn::Error::new_spanned(
                ident,
                format!(
                    "`{}` is in both a `unique(..)` set and a `project_scoped(unique(..))` \
                     set: a column is unique under one scope, so the impls would overlap",
                    quote!(#shared)
                ),
            ));
        }
        let row = addresses(
            ident,
            &global_sets,
            &format_ident!("AddressesOneRow"),
            Under::AnyScope,
        );
        let project_row = addresses(
            ident,
            &project_sets,
            &format_ident!("AddressesOneRow"),
            Under::InProject,
        );
        // The pk already pins a version: ANDing an instant onto it could only
        // turn a hit into a miss, so it addresses a row, never a version.
        let version = match (is_scd2, &partition) {
            (true, Some((_, partition_ty))) => {
                let partition_ty: Type = syn::parse_quote! { #partition_ty };
                let key = type_key(&partition_ty);
                let mut sets: Vec<Vec<Type>> = unique_sets
                    .iter()
                    .chain(project_sets.iter())
                    .chain(at_sets.iter())
                    .filter(|set| !set.iter().any(|t| type_key(t) == key))
                    .cloned()
                    .collect();
                sets.push(vec![partition_ty]);
                addresses(
                    ident,
                    &sets,
                    &format_ident!("AddressesOneVersion"),
                    Under::NoScope,
                )
            }
            _ => quote! {},
        };
        quote! { #row #project_row #version }
    };

    let recursive_impl = parsed
        .recursive
        .is_present()
        .then(|| {
            let up = role("recurse_up", "UP", "Key", "RecursiveDao")?;
            let down = role("recurse_down", "DOWN", "Key", "RecursiveDao")?;
            let ((up_col, _), (down_col, key_ty)) = up.zip(down).ok_or_else(|| {
                syn::Error::new_spanned(
                    ident,
                    "recursive: mark both #[dao(recursive(up))] and #[dao(recursive(down))]",
                )
            })?;
            Ok::<_, syn::Error>(quote! {
                impl ta_objects::RecursiveDao for #ident {
                    type Key = #key_ty;
                    const UP: Self::Iden = #up_col;
                    const DOWN: Self::Iden = #down_col;
                }
            })
        })
        .transpose()?;

    // `cfg(not(test))` keeps a crate's own test fixtures out of the registry
    // while dependency crates still register.
    let registration = ops.select.is_present().then(|| {
        let describe = if is_project_scoped {
            quote! { ta_objects::scoped_dao_projection }
        } else {
            quote! { ta_objects::dao_projection }
        };
        quote! {
            #[cfg(not(test))]
            ta_objects::inventory::submit! {
                ta_objects::RegisteredDao { describe: #describe::<#ident> }
            }
        }
    });

    Ok(quote! {
        #iden_enum
        #has_columns
        #fields_cb
        #dao_core
        #table_dao
        #reachable
        #insertable
        #pk_impl
        #addressing
        #select_impl
        #insert_impl
        #unversioned_impl
        #delete_impl
        #upsert_impl
        #update_impl
        #timeline_impl
        #recursive_impl
        #registration
    })
}

fn update_statement(f: &Field, marks: &FieldMarks, table_iden: &Ident) -> proc_macro2::TokenStream {
    let name = field_ident(f);
    let variant = iden_variant(name);
    if marks.has(f, "flatten") {
        return quote! {
            __values.extend(
                ta_objects::UpdateDao::update_values(&self.#name)
                    .into_iter()
                    .map(|(c, e)| (#table_iden::Base(c), e)),
            );
        };
    }
    if marks.has(f, "immutable") {
        return quote! {
            __values.push((
                #table_iden::#variant,
                sea_query::Expr::case(
                    sea_query::Expr::col(#table_iden::#variant).is_null(),
                    self.#name.as_sea_value()
                )
                .finally(sea_query::Expr::col(#table_iden::#variant))
                .into()
            ));
        };
    }
    if option_inner_type(&f.ty).is_some() {
        return quote! {
            if let Some(ref __v) = self.#name {
                __values.push((#table_iden::#variant, __v.as_sea_value().into()));
            }
        };
    }
    quote! {
        __values.push((#table_iden::#variant, self.#name.as_sea_value().into()));
    }
}

/// A set containing another adds nothing, and their impls would overlap.
fn minimal_sets(sets: Vec<Vec<Type>>) -> Vec<Vec<Type>> {
    let type_key = |t: &Type| quote!(#t).to_string();
    let keys: Vec<Vec<String>> = sets
        .iter()
        .map(|s| s.iter().map(type_key).collect())
        .collect();
    let mut minimal: Vec<Vec<Type>> = Vec::new();
    for (i, set) in sets.iter().enumerate() {
        let redundant = keys.iter().enumerate().any(|(j, other)| {
            i != j && other.len() < keys[i].len() && other.iter().all(|t| keys[i].contains(t))
        });
        let duplicate = minimal
            .iter()
            .any(|m| m.len() == set.len() && m.iter().map(type_key).eq(set.iter().map(type_key)));
        if !redundant && !duplicate {
            minimal.push(set.clone());
        }
    }
    minimal
}

pub fn iden_variant(field_ident: &Ident) -> Ident {
    format_ident!("{}", field_ident.to_string().to_upper_camel_case())
}

struct FlattenInfo {
    field: Ident,
    base_ty: syn::Path,
    base_iden: syn::Path,
}

impl FlattenInfo {
    fn base_last_ident(&self) -> Ident {
        self.base_ty
            .segments
            .last()
            .expect("non-empty type path")
            .ident
            .clone()
    }
}

fn flatten_info(marks: &FieldMarks) -> syn::Result<Option<FlattenInfo>> {
    let mut flattened = marks
        .fields_marked("flatten")
        .filter(|f| !type_names(&f.ty, "Scd2Interval"));
    let Some(f) = flattened.next() else {
        return Ok(None);
    };
    if let Some(second) = flattened.next() {
        return Err(syn::Error::new_spanned(
            second,
            "at most one #[dao(flatten)] base is supported (the interval aside)",
        ));
    }
    let Type::Path(tp) = &f.ty else {
        return Err(syn::Error::new_spanned(
            &f.ty,
            "#[dao(flatten)] field must be a plain DAO struct type",
        ));
    };
    let base_ty = tp.path.clone();
    let mut base_iden = base_ty.clone();
    let last = base_iden.segments.last_mut().expect("non-empty type path");
    last.ident = format_ident!("{}Iden", last.ident);
    Ok(Some(FlattenInfo {
        field: field_ident(f).clone(),
        base_ty,
        base_iden,
    }))
}

fn is_sqlx_skip(f: &Field) -> bool {
    f.attrs.iter().any(|a| {
        a.path().is_ident("sqlx")
            && a.parse_args::<Ident>()
                .map(|ident| ident == "skip")
                .unwrap_or(false)
    })
}

/// `UserDB` → `"User"`, `PhysicalFunctionRunDB` → `"Physical Function Run"`,
/// `FunctionDBWithNames` → `"Function"`; consecutive capitals stay joined.
fn struct_name_to_display_name(name: &str) -> String {
    let name = match name.find("DB") {
        Some(pos) if pos > 0 => &name[..pos],
        _ => name,
    };
    let mut result = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && c.is_uppercase() {
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
