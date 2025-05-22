//
// Copyright 2024 Tabs Data Inc.
//

use thiserror::Error;

use crate::logic::platform::component::parameters::ParameterError::MissingEnvironmentVariable;
use regex::{Error, Regex};
use std::env;

pub fn render(input: &str) -> Result<String, ParameterError> {
    let expression = Regex::new(r"\$\{(\w+)}")?;
    let mut output = String::new();
    let mut end = 0;
    for capture in expression.captures_iter(input) {
        let matching = capture.get(0).unwrap();
        let env_name = &capture[1];
        let env_value = match env::var(env_name) {
            Ok(value) => value,
            Err(_) => {
                return Err(MissingEnvironmentVariable {
                    name: env_name.to_string(),
                });
            }
        };
        output.push_str(&input[end..matching.start()]);
        output.push_str(&env_value);
        end = matching.end();
    }
    output.push_str(&input[end..]);
    Ok(output)
}

#[derive(Debug, Error)]
pub enum ParameterError {
    #[error("Argument could not be parsed for rendering: {0}")]
    InvalidParameterExpression(#[from] Error),
    #[error("Missing environment variable: {name}")]
    MissingEnvironmentVariable { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_with_valid_env_variables() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD1_PERSON", "Doteki");
        }
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD1_ACTIVITY", "Go Player");
        }
        let input = "Fact: ${TD1_PERSON} was a ${TD1_ACTIVITY}...";
        let output = render(input);
        assert_eq!(output.unwrap(), "Fact: Doteki was a Go Player...");
    }

    #[test]
    fn test_render_with_missing_env_variable() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD2_PERSON", "Frodo");
        }
        let input = "Fact: ${TD2_PERSON} was a ${TD2_ACTIVITY}...";
        let output = render(input);
        match output {
            Err(MissingEnvironmentVariable { name }) => {
                assert_eq!(name, "TD2_ACTIVITY");
            }
            _ => panic!("Expected MissingEnvironmentVariable error"),
        }
    }

    #[test]
    fn test_render_with_no_placeholders() {
        let input = "Fact: Wittgenstein was a Philosopher...";
        let result = render(input);
        assert_eq!(result.unwrap(), "Fact: Wittgenstein was a Philosopher...");
    }

    #[test]
    fn test_render_with_partial_placeholders() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD3_PERSON", "Euler");
        }
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD3_ACTIVITY", "Mathematician");
        }
        let input = "Fact: ${TD3_PERSON} was a ${TD3_LANGUAGE}...";
        let output = render(input);
        match output {
            Err(MissingEnvironmentVariable { name }) => {
                assert_eq!(name, "TD3_LANGUAGE");
            }
            _ => panic!("Expected MissingEnvironmentVariable error"),
        }
    }

    #[test]
    fn test_render_with_adjacent_placeholders() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD4_PERSON", "Lowenheim");
        }
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD4_ACTIVITY", "Logician");
        }
        let input = "...${TD4_PERSON}${TD4_ACTIVITY}...";
        let output = render(input);
        assert_eq!(output.unwrap(), "...LowenheimLogician...");
    }

    #[test]
    fn test_render_with_nested_placeholders() {
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD5_OUTER", "${TD5_INNER}");
        }
        // Setting env vars is not thread-safe; use with care.
        unsafe {
            env::set_var("TD5_INNER", "inner");
        }
        let input = "This is ${TD5_OUTER}.";
        let output = render(input);
        assert_eq!(output.unwrap(), "This is ${TD5_INNER}.");
    }
}
