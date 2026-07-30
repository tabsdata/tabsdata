//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    ConcreteResolver, LiteralOrigin, PATH_SEPARATOR, Resolution, ResolveContext, Rule,
};

pub(crate) struct LiteralResolver {
    auto: bool,
}

impl LiteralResolver {
    pub(crate) fn new(auto: bool) -> Self {
        Self { auto }
    }

    fn substitute(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        let token = context.placeholder.token();
        let provided = context
            .init_value
            .filter(|value| !value.is_empty() && *value != token);
        let category = context.placeholder.category.clone().unwrap_or_default();
        let memoized = self.memoized(context, &category);
        let value = match provided {
            Some(value) => self.remember(context, &category, value),
            None => match memoized {
                Some(value) => value,
                None => match context.auto_value.filter(|value| !value.is_empty()) {
                    Some(value) => value.to_owned(),
                    None if self.auto || context.placeholder.rule == Rule::Optional => {
                        String::new()
                    }
                    None => {
                        let path = context.path.join(PATH_SEPARATOR);
                        return Err(ResolverError::MissingRequiredValue {
                            path,
                            kind: self.kind().as_str().to_owned(),
                            category,
                        });
                    }
                },
            },
        };
        Ok(Resolution {
            rendered: value,
            secret: None,
        })
    }

    fn memoized(&self, context: &ResolveContext, category: &str) -> Option<String> {
        if category.is_empty() {
            return None;
        }
        context
            .literals
            .borrow()
            .get(category)
            .map(|origin| origin.value.clone())
    }

    fn remember(&self, context: &ResolveContext, category: &str, value: &str) -> String {
        if !category.is_empty() {
            context.literals.borrow_mut().insert(
                category.to_owned(),
                LiteralOrigin {
                    value: value.to_owned(),
                    path: context.path.join(PATH_SEPARATOR),
                },
            );
        }
        value.to_owned()
    }
}

impl ConcreteResolver for LiteralResolver {
    fn kind(&self) -> PlaceholderKind {
        if self.auto {
            PlaceholderKind::LiteralAuto
        } else {
            PlaceholderKind::Literal
        }
    }

    fn resolve(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        self.substitute(context)
    }

    fn materialize(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        self.substitute(context)
    }
}
