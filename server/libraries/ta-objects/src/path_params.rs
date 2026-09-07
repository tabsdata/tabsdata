//
// Copyright 2026 Tabsdata Inc.
//

use std::marker::PhantomData;

/// A URL param struct's path placeholders: names are the field names, in
/// declaration order. Derived by `#[ta_objects::UrlParam]`, so the struct a
/// handler decodes with is the same source of truth a URL is built from.
/// Tuples compose params of multi-variable routes, in path order.
pub trait PathParams {
    /// Placeholder names, in template order.
    fn names() -> Vec<&'static str>;
    /// Values, in `names()` order.
    fn values(&self) -> Vec<String>;
}

impl<A: PathParams, B: PathParams> PathParams for (A, B) {
    fn names() -> Vec<&'static str> {
        [A::names(), B::names()].concat()
    }
    fn values(&self) -> Vec<String> {
        [self.0.values(), self.1.values()].concat()
    }
}

/// A route template typed by the params it declares.
pub struct Route<P> {
    template: &'static str,
    _params: PhantomData<P>,
}

impl<P: PathParams> Route<P> {
    /// Panics when the template's placeholders differ from `P`'s — a wiring
    /// bug, surfaced at boot and in every test assembling the routes.
    pub fn new(template: &'static str) -> Self {
        let placeholders: Vec<&str> = template
            .split('/')
            .filter_map(|seg| seg.strip_prefix('{')?.strip_suffix('}'))
            .collect();
        assert_eq!(
            placeholders,
            P::names(),
            "route template `{template}` and its params disagree"
        );
        Self {
            template,
            _params: PhantomData,
        }
    }

    pub fn template(&self) -> &'static str {
        self.template
    }

    /// The concrete path, with the params substituted in.
    pub fn fill(&self, params: &P) -> String {
        let mut path = self.template.to_string();
        for (name, value) in P::names().into_iter().zip(params.values()) {
            let placeholder = format!("{{{name}}}");
            assert!(
                path.contains(&placeholder),
                "filling an unbound route template"
            );
            path = path.replace(&placeholder, &value);
        }
        path
    }
}

impl<P> Clone for Route<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P> Copy for Route<P> {}

impl<P> std::fmt::Debug for Route<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.template)
    }
}

/// The unbound test stand-in; filling it panics in debug builds.
impl<P> Default for Route<P> {
    fn default() -> Self {
        Self {
            template: "",
            _params: PhantomData,
        }
    }
}
