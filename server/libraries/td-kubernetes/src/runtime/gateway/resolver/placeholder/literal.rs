//
// Copyright 2026 Tabs Data Inc.
//

use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    ConcreteResolver, PATH_SEPARATOR, Resolution, ResolveContext, Rule,
};

pub(crate) struct LiteralResolver {
    auto: bool,
}

impl LiteralResolver {
    pub(crate) fn new(auto: bool) -> Self {
        Self { auto }
    }

    fn substitute(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        let value = match context.init_value {
            Some(value) => value.to_owned(),
            None if self.auto || context.placeholder.rule == Rule::Optional => String::new(),
            None => {
                let path = context.path.join(PATH_SEPARATOR);
                let category = context.placeholder.category.clone().unwrap_or_default();
                return Err(ResolverError::MissingRequiredValue {
                    path,
                    kind: self.kind().as_str().to_owned(),
                    category,
                });
            }
        };
        Ok(Resolution {
            rendered: value,
            secret: None,
        })
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
