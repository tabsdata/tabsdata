//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use serde_yaml::Value;

use crate::base::error::secret::SecretError;
use crate::runtime::secrets::walker::{Placeholder, SecretVisitor, walk};

pub use crate::runtime::secrets::walker::{Generator, SECRET_KEY_SEPARATOR, encode_secret_key};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretLeaf {
    pub path: Vec<String>,
    pub key: String,
    pub generator: Option<Generator>,
}

pub fn extract_secret_leaves(
    config: &str,
    yaml: &mut Value,
) -> Result<Vec<SecretLeaf>, SecretError> {
    let mut collector = LeafCollector { hits: Vec::new() };
    walk(config, yaml, &mut collector)?;

    let mut leaves: Vec<SecretLeaf> = Vec::with_capacity(collector.hits.len());
    let mut seen: HashMap<String, String> = HashMap::with_capacity(collector.hits.len());
    for (path, generator) in collector.hits {
        let key = encode_secret_key(config, &path)?;
        let displayed = path.join(".");
        if let Some(first) = seen.get(&key) {
            return Err(SecretError::KeyCollision {
                config: config.to_owned(),
                key,
                first: first.clone(),
                second: displayed,
            });
        }
        seen.insert(key.clone(), displayed);
        leaves.push(SecretLeaf {
            path,
            key,
            generator,
        });
    }
    Ok(leaves)
}

struct LeafCollector {
    hits: Vec<(Vec<String>, Option<Generator>)>,
}

impl SecretVisitor for LeafCollector {
    type Error = SecretError;

    fn visit_placeholder(
        &mut self,
        _config: &str,
        path: &[String],
        placeholder: Placeholder,
        _scalar: &mut String,
    ) -> Result<(), SecretError> {
        let generator = match placeholder {
            Placeholder::Bare => None,
            Placeholder::Generated(generator) => Some(generator),
        };
        self.hits.push((path.to_vec(), generator));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
