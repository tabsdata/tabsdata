//
// Copyright 2026 Tabs Data Inc.
//

use proc_macro::TokenStream;
use proc_macro2::TokenTree;
use proc_macro2::token_stream::IntoIter;
use quote::quote;
use std::iter::Peekable;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Ident, Token};

struct SqlMacroInput {
    builder: Option<Ident>,
    tokens: proc_macro2::TokenStream,
}

impl Parse for SqlMacroInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(Ident) && input.peek2(Token![,]) {
            let builder = Some(input.parse()?);
            input.parse::<Token![,]>()?;
            let tokens = input.parse()?;
            Ok(SqlMacroInput { builder, tokens })
        } else {
            let tokens = input.parse()?;
            Ok(SqlMacroInput {
                builder: None,
                tokens,
            })
        }
    }
}

/// Parse input tokens that can be a mix of string literals and {expr} blocks.
/// It generates a QueryBuilder with literals and bindings found in the parsed tokens.
/// It can also work on an already existing QueryBuilder passed in as the first argument.
/// Supports:
/// - String literals: " SELECT * FROM " -> sqlx::QueryBuilder::push()
/// - String variables: %{table} -> sqlx::QueryBuilder::push()
/// - Bindings: {user_id} -> sqlx::QueryBuilder::push_bind() (using SqlEntity::push_bind() to do so)
///   We could easily change how bindings are created.
/// - Code injection: [..] {..} -> supports arbitrary code injection with defined scope
///
/// Example:
/// ```rust
/// let (table, user_id, enabled) = ("users", 123, Some(true));
/// let mut builder = sql!(
///     "
///     SELECT * FROM " %{table} "
///     WHERE id = " {user_id}
///     [if let Some(e) = enabled] {
///         "AND enabled = " {e}
///     }
/// );
///
/// sql!(
///     builder,
///     " ORDER BY id"
/// )
///     .build_query_as()
///     .fetch_one(&db)
///     .await?;
/// ```
#[proc_macro]
pub fn sql(input: TokenStream) -> TokenStream {
    let parsed_input = syn::parse_macro_input!(input as SqlMacroInput);
    let tokens = parsed_input.tokens.into_iter().peekable();

    let (builder_ident, builder_creation) = if let Some(ident) = parsed_input.builder {
        (quote!(#ident), quote! {})
    } else {
        (
            quote!(builder),
            quote! {let mut builder = sqlx::QueryBuilder::default();},
        )
    };

    let expanded = sql_inner_expand(tokens, &builder_ident);

    // Wrap everything in a block and return the builder
    let expanded = quote! {
        {
            #builder_creation
            #expanded
            #builder_ident
        }
    };
    TokenStream::from(expanded)
}

fn flush_pending_literal(
    pending_literal: &mut String,
    builder_ident: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    if !pending_literal.is_empty() {
        let sql = normalize_sql(pending_literal);
        pending_literal.clear();
        quote! {
            #builder_ident.push(#sql);
        }
    } else {
        quote! {}
    }
}

fn normalize_sql(s: &str) -> String {
    // Remove only leading/trailing newlines and spaces
    s.trim_start_matches('\n')
        .trim_end_matches('\n')
        .to_string()
}

fn sql_inner_expand(
    mut tokens: Peekable<IntoIter>,
    builder_ident: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let mut statements = quote! {};
    let mut pending_literal = String::new();

    while let Some(token) = tokens.next() {
        match token {
            // --- String literal
            TokenTree::Literal(lit) => {
                let lit_str = lit.to_string();
                let sql_content = if lit_str.starts_with('"') && lit_str.ends_with('"') {
                    lit_str[1..lit_str.len() - 1].to_string()
                } else if lit_str.starts_with("r#\"") && lit_str.ends_with("\"#") {
                    lit_str[3..lit_str.len() - 2].to_string()
                } else if lit_str.starts_with("r\"") && lit_str.ends_with("\"") {
                    lit_str[2..lit_str.len() - 1].to_string()
                } else {
                    panic!(
                        "Expected string literal at {:?}: {}",
                        lit.span().start(),
                        lit_str
                    );
                };
                pending_literal.push_str(&sql_content);
            }

            // --- Direct SQL interpolation: %{expr}
            TokenTree::Punct(p) if p.as_char() == '%' => {
                statements.extend(flush_pending_literal(&mut pending_literal, builder_ident));

                // Expect the next token to be a group
                let next_token = tokens.next().expect("Expected `{…}` after `%`");
                if let TokenTree::Group(group) = next_token {
                    if group.delimiter() != proc_macro2::Delimiter::Brace {
                        panic!("Expected braces after `%` for SQL interpolation");
                    }

                    let expr: Expr = syn::parse2(group.stream())
                        .expect("Failed to parse expression inside `%{…}`");
                    statements.extend(quote! {
                        #builder_ident.push(#expr);
                    });
                } else {
                    panic!("Expected `{{…}}` after `%` for SQL interpolation");
                }
            }

            // --- Binding: {expr}
            TokenTree::Group(group) if group.delimiter() == proc_macro2::Delimiter::Brace => {
                statements.extend(flush_pending_literal(&mut pending_literal, builder_ident));

                let expr: Expr =
                    syn::parse2(group.stream()).expect("Failed to parse expression inside braces");
                statements.extend(quote! {
                    #expr.push_bind(&mut #builder_ident);
                });
            }

            TokenTree::Group(group) if group.delimiter() == proc_macro2::Delimiter::Bracket => {
                statements.extend(flush_pending_literal(&mut pending_literal, builder_ident));
                let cond_ts = group.stream(); // everything inside the brackets

                // Next token must be a { … } group
                let body_token = tokens
                    .next()
                    .expect("Expected `{…}` after `[...]` for Rust injection");
                if let TokenTree::Group(body_group) = body_token {
                    if body_group.delimiter() != proc_macro2::Delimiter::Brace {
                        panic!("Expected `{{…}}` after `[...]` for Rust injection");
                    }

                    // --- RECURSIVELY expand the content of the block ---
                    let body_tokens = body_group.stream().into_iter().peekable();
                    let expanded_body = sql_inner_expand(body_tokens, builder_ident);

                    statements.extend(quote! {
                        #cond_ts {
                            #expanded_body
                        }
                    });
                }
            }

            other => {
                panic!(
                    "Unexpected token in sql! macro at {:?}: {:?}",
                    other.span().start(),
                    other
                );
            }
        }
    }

    // Flush any remaining literal after the loop
    statements.extend(flush_pending_literal(&mut pending_literal, builder_ident));

    statements
}
