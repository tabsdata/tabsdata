//
// Copyright 2026 Tabs Data Inc.
//

/// Declarative macro that generates a [`TestRequirements`](super::TestRequirements)
/// implementation from a struct definition. Use this for every external-resource
/// provider — six near-identical hand-rolled impls (75 lines each) collapse to
/// ~8 lines.
///
/// The macro takes a struct with named fields where each field is paired with
/// the env-var key (after the `<NS>__` namespace prefix is stripped) that
/// populates it. All fields are `String`. A non-pub `vars` HashMap is added to
/// store the resolved map so the inherited `test_path` / `test_identifier`
/// trait methods can read framework-owned env vars.
///
/// # Example
///
/// ```ignore
/// td_test::define_requires! {
///     /// Requirements for a Postgres test using URI, user and password.
///     pub struct PostgresReqs {
///         uri: postgres_uri,
///         user: postgres_user,
///         password: postgres_password,
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_requires {
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident {
            $( $field:ident : $key:ident ),+ $(,)?
        }
    ) => {
        $(#[$attr])*
        #[allow(dead_code)]
        $vis struct $name {
            $( pub $field: ::std::string::String, )+
            vars: ::std::collections::HashMap<::std::string::String, ::std::string::String>,
        }

        impl $crate::reqs::TestRequirements for $name {
            fn keys() -> &'static [&'static str] {
                &[ $( stringify!($key) ),+ ]
            }

            fn new(
                vars: impl ::std::convert::Into<
                    ::std::collections::HashMap<::std::string::String, ::std::string::String>,
                >,
            ) -> Self {
                let vars = vars.into();
                Self {
                    $( $field: vars[stringify!($key)].clone(), )+
                    vars,
                }
            }

            fn vars(
                &self,
            ) -> &::std::collections::HashMap<::std::string::String, ::std::string::String> {
                &self.vars
            }
        }
    };
}
