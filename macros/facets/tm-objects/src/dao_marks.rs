//
// Copyright 2026 Tabsdata Inc.
//

use proc_macro2::Ident;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Field, Fields, Token, Type, parenthesized};

const MARKS: &[&str] = &[
    "pk",
    "partition_by",
    "scd2",
    "timeline",
    "recursive",
    "order_by",
    "immutable",
    "flatten",
];

/// Grain-owned marks nest under their grain's word; `timeline(version_by)`
/// is flattened to the role `version_by`, the grain kept for the cross-check.
/// `scd2` owns no field role: its interval is the `Scd2Interval` projection.
const GRAINS: &[(&str, &[(&str, &str)])] = &[
    ("scd2", &[]),
    ("timeline", &[("version_by", "version_by")]),
    (
        "recursive",
        &[("up", "recurse_up"), ("down", "recurse_down")],
    ),
];

pub struct Mark {
    pub name: Ident,
    pub arg: Option<Ident>,
    pub grain: Option<Ident>,
}

fn grain_roles(grain: &str) -> Option<&'static [(&'static str, &'static str)]> {
    GRAINS.iter().find(|(g, _)| *g == grain).map(|(_, r)| *r)
}

fn nested_form_of(role: &str) -> Vec<String> {
    GRAINS
        .iter()
        .flat_map(|(g, roles)| {
            roles
                .iter()
                .filter(move |(_, r)| *r == role)
                .map(move |(a, _)| format!("#[dao({g}({a}))]"))
        })
        .collect()
}

impl Mark {
    fn normalise(self) -> syn::Result<Self> {
        let name = self.name.to_string();
        let Some(roles) = grain_roles(&name) else {
            let bare = nested_form_of(&name);
            if !bare.is_empty() {
                return Err(syn::Error::new_spanned(
                    &self.name,
                    format!("`{name}` nests under its grain: {}", bare.join(" or ")),
                ));
            }
            return Ok(self);
        };
        let Some(arg) = self.arg else {
            return Err(syn::Error::new_spanned(
                &self.name,
                format!("`{name}` takes one of {}", expected(roles)),
            ));
        };
        let Some((_, role)) = roles.iter().find(|(a, _)| arg == a) else {
            return Err(syn::Error::new_spanned(
                &arg,
                format!("`{name}` takes one of {}, not `{arg}`", expected(roles)),
            ));
        };
        Ok(Mark {
            name: Ident::new(role, arg.span()),
            arg: None,
            grain: Some(self.name),
        })
    }
}

fn expected(roles: &[(&str, &str)]) -> String {
    roles
        .iter()
        .map(|(a, _)| format!("`{a}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

impl Parse for Mark {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let arg = if input.peek(syn::token::Paren) {
            let inner;
            parenthesized!(inner in input);
            Some(inner.parse()?)
        } else {
            None
        };
        Ok(Mark {
            name,
            arg,
            grain: None,
        })
    }
}

pub struct FieldMarks<'a> {
    entries: Vec<(&'a Field, Vec<Mark>)>,
}

impl<'a> FieldMarks<'a> {
    pub fn parse(fields: &'a Fields) -> syn::Result<Self> {
        let mut entries = Vec::new();
        for field in fields {
            let mut marks = Vec::new();
            for attr in field.attrs.iter().filter(|a| a.path().is_ident("dao")) {
                for mark in attr.parse_args_with(Punctuated::<Mark, Token![,]>::parse_terminated)? {
                    let name = mark.name.to_string();
                    if name == "unique" || name == "key" {
                        let ty = &field.ty;
                        return Err(syn::Error::new_spanned(
                            &mark.name,
                            format!(
                                "`{name}` is a struct-level argument, not a field mark: \
                                 #[dao(unique({}))]",
                                quote::quote! { #ty }
                            ),
                        ));
                    }
                    let mark = mark.normalise()?;
                    if !MARKS.contains(&name.as_str()) && mark.grain.is_none() {
                        return Err(syn::Error::new_spanned(
                            &mark.name,
                            format!(
                                "unknown field mark `{name}`; expected one of {}",
                                MARKS.join(", ")
                            ),
                        ));
                    }
                    marks.push(mark);
                }
            }
            entries.push((field, marks));
        }
        Ok(Self { entries })
    }

    pub fn check_grains(&self, declared: &[&str]) -> syn::Result<()> {
        for (_, marks) in &self.entries {
            for mark in marks {
                if let Some(grain) = &mark.grain
                    && !declared.iter().any(|d| grain == d)
                {
                    return Err(syn::Error::new_spanned(
                        grain,
                        format!("#[dao({grain}(..))] requires the struct to declare `{grain}`"),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn has(&self, field: &Field, name: &str) -> bool {
        self.entries
            .iter()
            .find(|(f, _)| std::ptr::eq(*f, field))
            .is_some_and(|(_, marks)| marks.iter().any(|m| m.name == name))
    }

    pub fn fields_marked<'m>(&'m self, name: &'m str) -> impl Iterator<Item = &'a Field> + 'm {
        self.entries
            .iter()
            .filter(move |(_, marks)| marks.iter().any(|m| m.name == name))
            .map(|(f, _)| *f)
    }

    pub fn marked_once(&self, name: &str) -> syn::Result<Option<&'a Field>> {
        let mut marked = self.fields_marked(name);
        let first = marked.next();
        if let Some(second) = marked.next() {
            let nested = nested_form_of(name);
            let spelled = if nested.is_empty() {
                format!("#[dao({name})]")
            } else {
                nested.join(" / ")
            };
            return Err(syn::Error::new_spanned(
                second,
                format!("at most one field may be marked {spelled}"),
            ));
        }
        Ok(first)
    }

    pub fn order_by(&self) -> syn::Result<Option<(&'a Ident, bool)>> {
        let Some(field) = self.marked_once("order_by")? else {
            return Ok(None);
        };
        let arg = self
            .entries
            .iter()
            .find(|(f, _)| std::ptr::eq(*f, field))
            .and_then(|(_, marks)| marks.iter().find(|m| m.name == "order_by"))
            .and_then(|m| m.arg.as_ref());
        let asc = match arg {
            None => false,
            Some(dir) if dir == "asc" => true,
            Some(dir) if dir == "desc" => false,
            Some(dir) => {
                return Err(syn::Error::new_spanned(
                    dir,
                    "order_by takes `asc` or `desc`",
                ));
            }
        };
        Ok(Some((field_ident(field), asc)))
    }
}

pub fn field_ident(field: &Field) -> &Ident {
    field.ident.as_ref().expect("named field")
}

pub fn field_type(field: &Field) -> &Type {
    option_inner_type(&field.ty).unwrap_or(&field.ty)
}

pub fn option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }
    None
}

pub fn type_names(ty: &Type, name: &str) -> bool {
    let Type::Path(tp) = ty else { return false };
    let Some(seg) = tp.path.segments.last() else {
        return false;
    };
    if seg.ident == name {
        return true;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return false;
    };
    args.args.iter().any(|a| match a {
        syn::GenericArgument::Type(inner) => type_names(inner, name),
        _ => false,
    })
}
