//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedFileName {
    pub name: String,
    pub tokens: Vec<String>,
    pub extensions: String,
}

impl ParsedFileName {
    pub fn render(&self) -> String {
        let mut rendered = String::with_capacity(
            self.name.len()
                + self
                    .tokens
                    .iter()
                    .map(|token| token.len() + 1)
                    .sum::<usize>()
                + self.extensions.len(),
        );
        rendered.push_str(&self.name);
        for token in &self.tokens {
            rendered.push('_');
            rendered.push_str(token);
        }
        rendered.push_str(&self.extensions);
        rendered
    }

    pub fn render_stripped(&self) -> String {
        let mut rendered = String::with_capacity(self.name.len() + self.extensions.len());
        rendered.push_str(&self.name);
        rendered.push_str(&self.extensions);
        rendered
    }
}

pub fn parse_filename(filename: &str) -> ParsedFileName {
    let (head, extensions) = match filename.find('.') {
        Some(dot) => (&filename[..dot], &filename[dot..]),
        None => (filename, ""),
    };
    let mut segments = head.split('_');
    let name = segments.next().unwrap_or("").to_owned();
    let tokens = segments.map(str::to_owned).collect();
    ParsedFileName {
        name,
        tokens,
        extensions: extensions.to_owned(),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Axis {
    pub name: &'static str,
    pub known: &'static [&'static str],
}

pub fn pick_winner(
    candidates: &[ParsedFileName],
    axes: &[Axis],
    preferred: &[&'static str],
) -> Option<usize> {
    let mut best: Option<(usize, usize)> = None;
    for (index, candidate) in candidates.iter().enumerate() {
        let Some(specificity) = score_candidate(candidate, axes, preferred) else {
            continue;
        };
        match best {
            Some((_, current)) if current >= specificity => {}
            _ => best = Some((index, specificity)),
        }
    }
    best.map(|(index, _)| index)
}

fn score_candidate(
    candidate: &ParsedFileName,
    axes: &[Axis],
    preferred: &[&'static str],
) -> Option<usize> {
    if candidate.tokens.len() > axes.len() {
        return None;
    }
    for (position, token) in candidate.tokens.iter().enumerate() {
        if token != preferred[position] {
            return None;
        }
    }
    Some(candidate.tokens.len())
}

pub fn group_by_key(
    filenames: &[String],
) -> BTreeMap<(String, String), Vec<(usize, ParsedFileName)>> {
    let mut groups: BTreeMap<(String, String), Vec<(usize, ParsedFileName)>> = BTreeMap::new();
    for (index, filename) in filenames.iter().enumerate() {
        let parsed = parse_filename(filename);
        let key = (parsed.name.clone(), parsed.extensions.clone());
        groups.entry(key).or_default().push((index, parsed));
    }
    groups
}

#[cfg(test)]
mod tests;
