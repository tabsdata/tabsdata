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
    "valid_from",
    "valid_to",
    "version_by",
    "order_by",
    "recurse_up",
    "recurse_down",
    "immutable",
    "flatten",
];

pub struct Mark {
    pub name: Ident,
    pub arg: Option<Ident>,
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
        Ok(Mark { name, arg })
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
                marks
                    .extend(attr.parse_args_with(Punctuated::<Mark, Token![,]>::parse_terminated)?);
            }
            for mark in &marks {
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
                if !MARKS.contains(&name.as_str()) {
                    return Err(syn::Error::new_spanned(
                        &mark.name,
                        format!(
                            "unknown field mark `{name}`; expected one of {}",
                            MARKS.join(", ")
                        ),
                    ));
                }
            }
            entries.push((field, marks));
        }
        Ok(Self { entries })
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

    pub fn any(&self, name: &str) -> bool {
        self.fields_marked(name).next().is_some()
    }

    pub fn marked_once(&self, name: &str) -> syn::Result<Option<&'a Field>> {
        let mut marked = self.fields_marked(name);
        let first = marked.next();
        if let Some(second) = marked.next() {
            return Err(syn::Error::new_spanned(
                second,
                format!("at most one field may be marked #[dao({name})]"),
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
