//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::LazyLock;

use ratatui::style::{Modifier, Style};
use regex::Regex;

use super::palette::palette;

const ENV_PREFIX: &str = "env:";
const EMPTY_VALUES: [&str; 2] = ["\"\"", "''"];
const PLAIN_SCALARS: [&str; 4] = ["true", "false", "null", "~"];

static LINE_REFERENCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bline (\d+)").expect("valid pattern"));
static QUOTED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"'([^']+)'").expect("valid pattern"));

pub fn highlight(text: &str) -> Vec<Style> {
    let colors = palette();
    let characters: Vec<char> = text.chars().collect();
    let mut styles = vec![Style::default().fg(colors.text); characters.len()];
    let mut paint = |from: usize, to: usize, style: Style| {
        for slot in styles.iter_mut().take(to).skip(from) {
            *slot = style;
        }
    };
    let indent = characters.iter().take_while(|c| c.is_whitespace()).count();
    if characters.get(indent) == Some(&'#') {
        paint(
            indent,
            characters.len(),
            Style::default()
                .fg(colors.comment)
                .add_modifier(Modifier::ITALIC),
        );
        return styles;
    }
    let mut start = indent;
    if characters.get(indent) == Some(&'-') && characters.get(indent + 1) == Some(&' ') {
        paint(indent, indent + 1, Style::default().fg(colors.punctuation));
        start = indent + 2;
    }
    let Some(colon) = key_end(&characters, start) else {
        paint_value(&characters, start, &mut paint);
        return styles;
    };
    let section = characters[colon + 1..].iter().all(|c| c.is_whitespace());
    let key_color = if section { colors.section } else { colors.key };
    paint(
        start,
        colon,
        Style::default().fg(key_color).add_modifier(Modifier::BOLD),
    );
    paint(
        colon,
        colon + 1,
        Style::default()
            .fg(colors.punctuation)
            .add_modifier(Modifier::BOLD),
    );
    paint_value(&characters, colon + 1, &mut paint);
    styles
}

pub fn empty_values(lines: &[String]) -> usize {
    lines
        .iter()
        .filter(|line| value_of(line).is_some_and(|value| EMPTY_VALUES.contains(&value)))
        .count()
}

pub fn first_empty_value(lines: &[String]) -> Option<usize> {
    lines
        .iter()
        .position(|line| value_of(line).is_some_and(|value| EMPTY_VALUES.contains(&value)))
}

pub fn locate(message: &str, lines: &[String]) -> Option<usize> {
    if let Some(line) = LINE_REFERENCE
        .captures(message)
        .and_then(|captures| captures[1].parse::<usize>().ok())
        .filter(|line| (1..=lines.len()).contains(line))
    {
        return Some(line - 1);
    }
    QUOTED.captures_iter(message).find_map(|captures| {
        let quoted = &captures[1];
        lines.iter().position(|line| {
            key_of(line).is_some_and(|key| {
                quoted == key
                    || quoted.ends_with(&format!(".{key}"))
                    || quoted.ends_with(&format!(":{key}"))
            })
        })
    })
}

fn key_of(line: &str) -> Option<String> {
    let characters: Vec<char> = line.chars().collect();
    let start = characters.iter().take_while(|c| c.is_whitespace()).count();
    let colon = key_end(&characters, start)?;
    Some(characters[start..colon].iter().collect::<String>())
}

fn value_of(line: &str) -> Option<&str> {
    let characters: Vec<char> = line.chars().collect();
    let start = characters.iter().take_while(|c| c.is_whitespace()).count();
    let colon = key_end(&characters, start)?;
    let offset = line.char_indices().nth(colon + 1).map(|(index, _)| index)?;
    Some(line[offset..].trim())
}

fn key_end(characters: &[char], start: usize) -> Option<usize> {
    let mut quote: Option<char> = None;
    for (position, character) in characters.iter().enumerate().skip(start) {
        match (quote, character) {
            (None, '#') => return None,
            (None, '"' | '\'') => quote = Some(*character),
            (Some(open), _) if *character == open => quote = None,
            (None, ':')
                if characters
                    .get(position + 1)
                    .is_none_or(|next| next.is_whitespace()) =>
            {
                return Some(position);
            }
            _ => {}
        }
    }
    None
}

fn paint_value(characters: &[char], from: usize, paint: &mut impl FnMut(usize, usize, Style)) {
    let colors = palette();
    let start = from
        + characters[from..]
            .iter()
            .take_while(|c| c.is_whitespace())
            .count();
    if start >= characters.len() {
        return;
    }
    let value: String = characters[start..].iter().collect();
    let trimmed = value.trim_end();
    let end = start + trimmed.chars().count();
    if EMPTY_VALUES.contains(&trimmed) {
        paint(
            start,
            end,
            Style::default()
                .fg(colors.base)
                .bg(colors.required)
                .add_modifier(Modifier::BOLD),
        );
    } else if trimmed.starts_with(ENV_PREFIX) {
        let prefix = start + ENV_PREFIX.len();
        paint(
            start,
            prefix,
            Style::default()
                .fg(colors.reference)
                .add_modifier(Modifier::BOLD),
        );
        let reference: String = characters[prefix..end].iter().collect();
        if reference.contains("${") {
            let mut inside = false;
            for (position, character) in reference.chars().enumerate() {
                let next = reference.chars().nth(position + 1);
                if !inside && character == '$' && next == Some('{') {
                    inside = true;
                }
                let color = if inside {
                    colors.variable
                } else {
                    colors.string
                };
                paint(
                    prefix + position,
                    prefix + position + 1,
                    Style::default().fg(color),
                );
                if inside && character == '}' {
                    inside = false;
                }
            }
        } else {
            paint(prefix, end, Style::default().fg(colors.variable));
        }
    } else if trimmed.parse::<f64>().is_ok() || PLAIN_SCALARS.contains(&trimmed) {
        paint(start, end, Style::default().fg(colors.scalar));
    } else {
        paint(start, end, Style::default().fg(colors.string));
    }
}
