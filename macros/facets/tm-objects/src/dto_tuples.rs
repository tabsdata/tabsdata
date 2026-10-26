//
// Copyright 2026 Tabsdata Inc.
//

use syn::{Fields, GenericArgument, PathArguments, Type, TypeTuple};

const TUPLE_IN_DTO: &str = "tuples are not allowed in Dto fields: utoipa inlines a tuple's \
     elements as `prefixItems` and drops the schema references inside them, so any `$ref` an \
     element carries is missing from `components.schemas` and the served OpenAPI document does \
     not resolve. Declare a `#[ta_objects::Dto]` struct with one named field per element instead.";

pub fn reject_tuples(fields: &Fields) -> syn::Result<()> {
    match fields.iter().find_map(|f| tuple_in(&f.ty)) {
        Some(tuple) => Err(syn::Error::new_spanned(tuple, TUPLE_IN_DTO)),
        None => Ok(()),
    }
}

pub fn tuple_in(ty: &Type) -> Option<&TypeTuple> {
    match ty {
        Type::Tuple(tuple) if !tuple.elems.is_empty() => Some(tuple),
        Type::Tuple(_) => None,
        Type::Path(path) => path
            .path
            .segments
            .iter()
            .find_map(|seg| match &seg.arguments {
                PathArguments::AngleBracketed(args) => args.args.iter().find_map(|arg| match arg {
                    GenericArgument::Type(inner) => tuple_in(inner),
                    _ => None,
                }),
                _ => None,
            }),
        Type::Array(array) => tuple_in(&array.elem),
        Type::Slice(slice) => tuple_in(&slice.elem),
        Type::Reference(reference) => tuple_in(&reference.elem),
        Type::Paren(paren) => tuple_in(&paren.elem),
        Type::Group(group) => tuple_in(&group.elem),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::tuple_in;
    use syn::{Type, parse_str};

    fn has_tuple(ty: &str) -> bool {
        tuple_in(&parse_str::<Type>(ty).unwrap()).is_some()
    }

    #[test]
    fn finds_tuples_at_any_depth() {
        assert!(has_tuple("(A, B)"));
        assert!(has_tuple("Vec<(A, B, C)>"));
        assert!(has_tuple("Option<HashMap<K, (A, B)>>"));
        assert!(has_tuple("Box<[(A, B)]>"));
        assert!(has_tuple("[(A, B); 2]"));
        assert!(has_tuple("&'a (A, B)"));
    }

    #[test]
    fn allows_everything_else() {
        assert!(!has_tuple("()"));
        assert!(!has_tuple("Vec<()>"));
        assert!(!has_tuple("Vec<GraphRelation<TableVersions>>"));
        assert!(!has_tuple(
            "HashMap<FunctionVersionId, FunctionNodeResponse>"
        ));
        assert!(!has_tuple("Option<Option<String>>"));
    }
}
