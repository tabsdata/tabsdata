//
// Copyright 2026 Tabsdata Inc.
//

/// Declarative macro that generates a [`Backend`](super::Backend) implementation
/// from a struct definition — the Rust counterpart of the Python fixture
/// dataclasses in `tabsdatax.itest.fixture.backends`.
///
/// Each field is read from the descriptor key of the same **upper-cased** name,
/// exactly as the Python `_construct` does. Field types implement
/// [`FromAttr`](super::FromAttr): `String` and `PathBuf` are required keys,
/// `Option<String>` tolerates an absent one (`MODEL` / `DATA` before
/// create-model), and `Option<BTreeMap<String, i64>>` receives the domain's
/// `entities` map.
///
/// # Example
///
/// ```ignore
/// td_test::define_backend! {
///     /// Fixture struct for an `s3` domain.
///     pub struct S3Domain: domain("s3") {
///         name: String,
///         bucket: String,
///         base_path: PathBuf,
///         model: Option<String>,
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_backend {
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident : $role:ident ( $system_type:literal ) {
            $( $(#[$field_attr:meta])* $field:ident : $ty:ty ),+ $(,)?
        }
    ) => {
        $(#[$attr])*
        #[derive(Debug, Clone)]
        #[allow(dead_code)]
        $vis struct $name {
            $( $(#[$field_attr])* pub $field: $ty, )+
        }

        impl $crate::domain::Backend for $name {
            const SYSTEM_TYPE: &'static str = $system_type;
            const ROLE: $crate::domain::Role = $crate::define_backend!(@role $role);

            fn build(
                attrs: &$crate::domain::Attrs,
            ) -> ::std::result::Result<Self, ::std::string::String> {
                // Collect every missing key so the failure names them all at
                // once rather than one re-run at a time.
                let mut missing: ::std::vec::Vec<::std::string::String> = ::std::vec::Vec::new();
                $(
                    let $field = match <$ty as $crate::domain::FromAttr>::from_attr(
                        attrs,
                        &stringify!($field).to_uppercase(),
                    ) {
                        ::std::result::Result::Ok(value) => ::std::option::Option::Some(value),
                        ::std::result::Result::Err(key) => {
                            missing.push(key);
                            ::std::option::Option::None
                        }
                    };
                )+
                if !missing.is_empty() {
                    return ::std::result::Result::Err(missing.join(", "));
                }
                ::std::result::Result::Ok(Self {
                    $( $field: $field.expect("checked above"), )+
                })
            }
        }

        $crate::define_backend!(@role_marker $role $name);
    };

    (@role system) => { $crate::domain::Role::System };
    (@role domain) => { $crate::domain::Role::Domain };

    (@role_marker system $name:ident) => {
        impl $crate::domain::SystemBackend for $name {}
    };
    (@role_marker domain $name:ident) => {
        impl $crate::domain::DomainBackend for $name {}
    };
}
