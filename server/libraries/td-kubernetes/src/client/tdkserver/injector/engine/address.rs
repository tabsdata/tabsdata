//
// Copyright 2026 Tabsdata Inc.
//

use std::fmt;
use std::iter::Peekable;
use std::str::{Chars, FromStr};

use crate::runtime::gateway::resolver::kind::PlaceholderKind;

pub const CONFIG_EXTENSION: &str = ".yaml";

const ADDRESS_SEPARATOR: char = ':';
const ASSIGNMENT: char = '=';
const PATH_SEPARATOR: char = '.';
const SELECTOR_OPENER: char = '[';
const SELECTOR_CLOSER: char = ']';
const QUOTE: char = '"';
const ESCAPE: char = '\\';

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    Category {
        kind: Option<PlaceholderKind>,
        category: String,
    },
    Key {
        file: String,
        path: Vec<Segment>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub key: String,
    pub selectors: Vec<Selector>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    Index(usize),
    Match { key: String, value: String },
}

impl Address {
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("the address is empty".to_owned());
        }
        match split_outside(text, ADDRESS_SEPARATOR) {
            Some((prefix, rest)) => match PlaceholderKind::from_str(prefix) {
                Ok(kind) => Ok(Self::Category {
                    kind: Some(kind),
                    category: parse_category(rest)?,
                }),
                Err(()) => Ok(Self::Key {
                    file: normalize_file(prefix)?,
                    path: parse_path(rest)?,
                }),
            },
            None => Ok(Self::Category {
                kind: None,
                category: parse_category(text)?,
            }),
        }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Category {
                kind: Some(kind),
                category,
            } => write!(formatter, "{}{ADDRESS_SEPARATOR}{category}", kind.as_str()),
            Self::Category {
                kind: None,
                category,
            } => write!(formatter, "{category}"),
            Self::Key { file, path } => {
                write!(formatter, "{file}{ADDRESS_SEPARATOR}{}", display_path(path))
            }
        }
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reserved = [
            PATH_SEPARATOR,
            SELECTOR_OPENER,
            SELECTOR_CLOSER,
            QUOTE,
            ESCAPE,
            ADDRESS_SEPARATOR,
            ASSIGNMENT,
        ];
        if self.key.is_empty() || self.key.contains(reserved) {
            write!(formatter, "{QUOTE}")?;
            for character in self.key.chars() {
                if character == QUOTE || character == ESCAPE {
                    write!(formatter, "{ESCAPE}")?;
                }
                write!(formatter, "{character}")?;
            }
            write!(formatter, "{QUOTE}")?;
        } else {
            write!(formatter, "{}", self.key)?;
        }
        for selector in &self.selectors {
            write!(formatter, "{selector}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index(index) => write!(formatter, "{SELECTOR_OPENER}{index}{SELECTOR_CLOSER}"),
            Self::Match { key, value } => write!(
                formatter,
                "{SELECTOR_OPENER}{key}{ASSIGNMENT}{value}{SELECTOR_CLOSER}"
            ),
        }
    }
}

pub fn display_path(path: &[Segment]) -> String {
    path.iter()
        .map(Segment::to_string)
        .collect::<Vec<_>>()
        .join(&PATH_SEPARATOR.to_string())
}

pub fn split_assignment(text: &str) -> Option<(&str, &str)> {
    split_outside(text, ASSIGNMENT)
}

pub fn normalize_file(file: &str) -> Result<String, String> {
    let file = file.trim();
    if file.is_empty() {
        return Err("the file name is empty".to_owned());
    }
    if file.contains(['/', '\\']) {
        return Err(format!(
            "the name '{file}' is a path, not a config file name"
        ));
    }
    if file.ends_with(CONFIG_EXTENSION) {
        Ok(file.to_owned())
    } else {
        Ok(format!("{file}{CONFIG_EXTENSION}"))
    }
}

fn parse_category(text: &str) -> Result<String, String> {
    let category = text.trim();
    if category.is_empty() {
        return Err("the placeholder category is empty".to_owned());
    }
    let valid = category
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'));
    if !valid {
        return Err(format!(
            "the text '{category}' is not a placeholder category; a config key is \
             addressed as 'file:key.path'"
        ));
    }
    Ok(category.to_owned())
}

pub fn parse_path(text: &str) -> Result<Vec<Segment>, String> {
    let text = text.trim();
    let mut characters = text.chars().peekable();
    if characters.peek().is_none() {
        return Err("the key path is empty".to_owned());
    }
    let mut segments = Vec::new();
    loop {
        let key = if characters.peek() == Some(&QUOTE) {
            characters.next();
            parse_quoted(&mut characters, text)?
        } else {
            parse_bare(&mut characters, text)?
        };
        let mut selectors = Vec::new();
        while characters.peek() == Some(&SELECTOR_OPENER) {
            characters.next();
            let mut inner = String::new();
            loop {
                match characters.next() {
                    Some(SELECTOR_CLOSER) => break,
                    Some(character) => inner.push(character),
                    None => return Err(format!("the key path '{text}' has an unclosed selector")),
                }
            }
            selectors.push(parse_selector(&inner, text)?);
        }
        segments.push(Segment { key, selectors });
        match characters.next() {
            None => return Ok(segments),
            Some(PATH_SEPARATOR) => {}
            Some(other) => {
                return Err(format!("the key path '{text}' has an unexpected '{other}'"));
            }
        }
    }
}

fn parse_bare(characters: &mut Peekable<Chars>, text: &str) -> Result<String, String> {
    let mut key = String::new();
    while let Some(&character) = characters.peek() {
        if matches!(
            character,
            PATH_SEPARATOR | SELECTOR_OPENER | SELECTOR_CLOSER | QUOTE
        ) {
            break;
        }
        key.push(character);
        characters.next();
    }
    if key.is_empty() {
        return Err(format!("the key path '{text}' has an empty segment"));
    }
    Ok(key)
}

fn parse_quoted(characters: &mut Peekable<Chars>, text: &str) -> Result<String, String> {
    let mut key = String::new();
    loop {
        match characters.next() {
            Some(QUOTE) => return Ok(key),
            Some(ESCAPE) => match characters.next() {
                Some(character) => key.push(character),
                None => break,
            },
            Some(character) => key.push(character),
            None => break,
        }
    }
    Err(format!(
        "the key path '{text}' has an unclosed quoted segment"
    ))
}

fn parse_selector(inner: &str, text: &str) -> Result<Selector, String> {
    if let Ok(index) = inner.trim().parse::<usize>() {
        return Ok(Selector::Index(index));
    }
    match inner.split_once(ASSIGNMENT) {
        Some((key, value)) if !key.trim().is_empty() => Ok(Selector::Match {
            key: key.trim().to_owned(),
            value: value.trim().to_owned(),
        }),
        _ => Err(format!(
            "the key path '{text}' has selector '[{inner}]', but a selector is '[index]' or \
             '[key=value]'"
        )),
    }
}

fn split_outside(text: &str, separator: char) -> Option<(&str, &str)> {
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in text.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            ESCAPE if quoted => escaped = true,
            QUOTE => quoted = !quoted,
            SELECTOR_OPENER if !quoted => depth += 1,
            SELECTOR_CLOSER if !quoted => depth = depth.saturating_sub(1),
            _ if character == separator && depth == 0 && !quoted => {
                return Some((&text[..index], &text[index + character.len_utf8()..]));
            }
            _ => {}
        }
    }
    None
}
