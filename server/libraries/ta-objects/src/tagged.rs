//
// Copyright 2026 Tabsdata Inc.
//

/// The tag prefix a value renders with under a specific `IdName` enum `E`.
///
/// `#[typed_tag]` enums (e.g. `ProjectIdName`) distinguish their variants by a
/// string prefix — `~` marks the id variant, the empty string a name — and
/// their `FromStr`/`Display` keep that convention: a bare string parses as a
/// *name*, only a `~`-prefixed one as an *id*.
///
/// The `#[typed_tag]` macro implements this for the inner type of each
/// non-empty-tag variant, keyed by the enum `E` so the *same* inner type may
/// belong to more than one `IdName` (each with its own prefix) without a
/// conflicting impl. [`PREFIX`](Tagged::PREFIX) is the single source of that
/// tag — both the enum's `Display` and [`TaggedExt::tagged`] read it, so the
/// `~` literal never appears at a call site.
pub trait Tagged<E> {
    /// The `IdName` tag this value renders with under `E` (e.g. `"~"`).
    const PREFIX: &'static str;
}

/// Renders a value in the tagged spelling of a chosen `IdName`:
/// `id.tagged::<ProjectIdName>()` → `~<id>`, the form that `IdName` parses back
/// as an id rather than a name. Blanket-available; the `IdName` is named at the
/// call site so a type shared across several `IdName`s stays unambiguous.
pub trait TaggedExt: std::fmt::Display {
    fn tagged<E>(&self) -> String
    where
        Self: Tagged<E>,
    {
        format!("{}{}", <Self as Tagged<E>>::PREFIX, self)
    }
}

impl<T: std::fmt::Display> TaggedExt for T {}
