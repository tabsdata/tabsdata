//
// Copyright 2026 Tabsdata Inc.
//

use saphyr_parser::{Event, Parser, ScalarStyle, Span, SpannedEventReceiver};
use serde_yaml::Value;

use crate::client::tdkserver::injector::engine::address::{Segment, Selector};

const MAPPING_INDICATOR: char = ':';

#[derive(Debug)]
pub enum Node {
    Scalar(Scalar),
    Mapping(Vec<Entry>),
    Sequence(Vec<Node>),
    Alias,
}

#[derive(Debug)]
pub struct Entry {
    key: Option<String>,
    value: Node,
}

#[derive(Debug)]
pub struct Scalar {
    pub value: String,
    pub style: ScalarStyle,
    pub flow: bool,
    start: usize,
    end: usize,
    decorated: bool,
}

impl Scalar {
    pub fn is_empty_plain(&self) -> bool {
        self.style == ScalarStyle::Plain && self.value.is_empty() && self.start == self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Key(String),
    Index(usize),
}

pub struct Leaf<'a> {
    pub scalar: &'a Scalar,
    pub steps: Vec<Step>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Lookup {
    Missing,
    NotALeaf,
    Alias,
    Selector { selector: String, count: usize },
}

#[derive(Debug, Clone)]
pub struct Edit {
    pub start: usize,
    pub end: usize,
    pub text: String,
    pub steps: Vec<Step>,
}

pub struct Document {
    source: String,
    root: Option<Node>,
}

impl Document {
    pub fn parse(source: &str) -> Result<Self, String> {
        let mut builder = Builder::new(source);
        Parser::new_from_str(source)
            .load(&mut builder, false)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            source: source.to_owned(),
            root: builder.root,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn find(&self, path: &[Segment]) -> Result<Leaf<'_>, Lookup> {
        let mut node = self.root.as_ref().ok_or(Lookup::Missing)?;
        let mut steps = Vec::new();
        for segment in path {
            let Node::Mapping(entries) = node else {
                return Err(Lookup::Missing);
            };
            let entry = entries
                .iter()
                .find(|entry| entry.key.as_deref() == Some(segment.key.as_str()))
                .ok_or(Lookup::Missing)?;
            steps.push(Step::Key(segment.key.clone()));
            node = &entry.value;
            for selector in &segment.selectors {
                let Node::Sequence(items) = node else {
                    return Err(Lookup::Missing);
                };
                let (index, item) = select(items, selector)?;
                steps.push(Step::Index(index));
                node = item;
            }
        }
        match node {
            Node::Scalar(scalar) => Ok(Leaf { scalar, steps }),
            Node::Mapping(_) | Node::Sequence(_) => Err(Lookup::NotALeaf),
            Node::Alias => Err(Lookup::Alias),
        }
    }

    pub fn leaves(&self) -> Vec<Leaf<'_>> {
        let mut leaves = Vec::new();
        if let Some(root) = &self.root {
            collect_leaves(root, &mut Vec::new(), &mut leaves);
        }
        leaves
    }

    pub fn edit(&self, leaf: &Leaf, text: &str) -> Result<Edit, String> {
        let scalar = leaf.scalar;
        if scalar.decorated {
            return Err("it has an anchor or a tag".to_owned());
        }
        if matches!(scalar.style, ScalarStyle::Literal | ScalarStyle::Folded) {
            return Err("it is a block scalar".to_owned());
        }
        if scalar.is_empty_plain() {
            if !self.source[scalar.start..].starts_with(MAPPING_INDICATOR) {
                return Err("it is an empty list item".to_owned());
            }
            let position = scalar.start + MAPPING_INDICATOR.len_utf8();
            return Ok(Edit {
                start: position,
                end: position,
                text: format!(" {text}"),
                steps: leaf.steps.clone(),
            });
        }
        let delimited = serde_yaml::from_str::<Value>(&self.source[scalar.start..scalar.end])
            .ok()
            .and_then(|value| scalar_text(&value))
            .is_some_and(|value| value == scalar.value);
        if !delimited {
            return Err("its text in the file could not be delimited exactly".to_owned());
        }
        Ok(Edit {
            start: scalar.start,
            end: scalar.end,
            text: text.to_owned(),
            steps: leaf.steps.clone(),
        })
    }
}

pub fn apply(source: &str, edits: &[Edit]) -> String {
    let mut ordered: Vec<&Edit> = edits.iter().collect();
    ordered.sort_by_key(|edit| std::cmp::Reverse(edit.start));
    let mut result = source.to_owned();
    for edit in ordered {
        result.replace_range(edit.start..edit.end, &edit.text);
    }
    result
}

pub fn set_at(node: &mut Value, steps: &[Step], replacement: Value) -> bool {
    let Some((step, rest)) = steps.split_first() else {
        *node = replacement;
        return true;
    };
    let child = match (step, node) {
        (Step::Key(key), Value::Mapping(map)) => map
            .iter_mut()
            .find(|(candidate, _)| scalar_text(candidate).as_deref() == Some(key.as_str()))
            .map(|(_, child)| child),
        (Step::Index(index), Value::Sequence(items)) => items.get_mut(*index),
        _ => None,
    };
    match child {
        Some(child) => set_at(child, rest, replacement),
        None => false,
    }
}

fn select<'a>(items: &'a [Node], selector: &Selector) -> Result<(usize, &'a Node), Lookup> {
    match selector {
        Selector::Index(index) => items
            .get(*index)
            .map(|item| (*index, item))
            .ok_or(Lookup::Missing),
        Selector::Match { key, value } => {
            let matches: Vec<(usize, &Node)> = items
                .iter()
                .enumerate()
                .filter(|(_, item)| item_matches(item, key, value))
                .collect();
            match matches.as_slice() {
                [single] => Ok(*single),
                _ => Err(Lookup::Selector {
                    selector: selector.to_string(),
                    count: matches.len(),
                }),
            }
        }
    }
}

fn item_matches(item: &Node, key: &str, value: &str) -> bool {
    let Node::Mapping(entries) = item else {
        return false;
    };
    entries.iter().any(|entry| {
        entry.key.as_deref() == Some(key)
            && matches!(&entry.value, Node::Scalar(scalar) if scalar.value == value)
    })
}

fn collect_leaves<'a>(node: &'a Node, steps: &mut Vec<Step>, leaves: &mut Vec<Leaf<'a>>) {
    match node {
        Node::Scalar(scalar) => leaves.push(Leaf {
            scalar,
            steps: steps.clone(),
        }),
        Node::Mapping(entries) => {
            for entry in entries {
                let Some(key) = &entry.key else {
                    continue;
                };
                steps.push(Step::Key(key.clone()));
                collect_leaves(&entry.value, steps, leaves);
                steps.pop();
            }
        }
        Node::Sequence(items) => {
            for (index, item) in items.iter().enumerate() {
                steps.push(Step::Index(index));
                collect_leaves(item, steps, leaves);
                steps.pop();
            }
        }
        Node::Alias => {}
    }
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

fn quoted_end(source: &str, start: usize, style: ScalarStyle) -> usize {
    let mut characters = source[start..].char_indices().skip(1).peekable();
    while let Some((offset, character)) = characters.next() {
        match (style, character) {
            (ScalarStyle::DoubleQuoted, '\\') => {
                characters.next();
            }
            (ScalarStyle::DoubleQuoted, '"') => return start + offset + 1,
            (ScalarStyle::SingleQuoted, '\'') => {
                if characters.peek().is_some_and(|(_, next)| *next == '\'') {
                    characters.next();
                } else {
                    return start + offset + 1;
                }
            }
            _ => {}
        }
    }
    source.len()
}

enum Frame {
    Mapping {
        entries: Vec<Entry>,
        pending: Option<Option<String>>,
        flow: bool,
    },
    Sequence {
        items: Vec<Node>,
        flow: bool,
    },
}

impl Frame {
    fn flow(&self) -> bool {
        match self {
            Self::Mapping { flow, .. } | Self::Sequence { flow, .. } => *flow,
        }
    }
}

struct Builder<'s> {
    source: &'s str,
    offsets: Vec<usize>,
    frames: Vec<Frame>,
    root: Option<Node>,
}

impl<'s> Builder<'s> {
    fn new(source: &'s str) -> Self {
        let mut offsets: Vec<usize> = source.char_indices().map(|(offset, _)| offset).collect();
        offsets.push(source.len());
        Self {
            source,
            offsets,
            frames: Vec::new(),
            root: None,
        }
    }

    fn byte(&self, char_index: usize) -> usize {
        self.offsets
            .get(char_index)
            .copied()
            .unwrap_or_else(|| self.offsets.last().copied().unwrap_or_default())
    }

    fn in_flow(&self) -> bool {
        self.frames.last().is_some_and(Frame::flow)
    }

    fn complete(&mut self, node: Node) {
        match self.frames.last_mut() {
            None => self.root = Some(node),
            Some(Frame::Sequence { items, .. }) => items.push(node),
            Some(Frame::Mapping {
                entries, pending, ..
            }) => match pending.take() {
                None => {
                    let key = match node {
                        Node::Scalar(scalar) => Some(scalar.value),
                        _ => None,
                    };
                    *pending = Some(key);
                }
                Some(key) => entries.push(Entry { key, value: node }),
            },
        }
    }
}

impl<'input> SpannedEventReceiver<'input> for Builder<'_> {
    fn on_event(&mut self, event: Event<'input>, span: Span) {
        match event {
            Event::Scalar(value, style, anchor, tag) => {
                let start = self.byte(span.start.index());
                let end = match style {
                    ScalarStyle::SingleQuoted | ScalarStyle::DoubleQuoted => {
                        quoted_end(self.source, start, style)
                    }
                    _ => self.byte(span.end.index()),
                };
                let scalar = Scalar {
                    value: value.into_owned(),
                    style,
                    flow: self.in_flow(),
                    start,
                    end,
                    decorated: anchor != 0 || tag.is_some(),
                };
                self.complete(Node::Scalar(scalar));
            }
            Event::Alias(_) => self.complete(Node::Alias),
            Event::MappingStart(..) => {
                let flow = self.in_flow() || span.end.index() > span.start.index();
                self.frames.push(Frame::Mapping {
                    entries: Vec::new(),
                    pending: None,
                    flow,
                });
            }
            Event::SequenceStart(..) => {
                let flow = self.in_flow() || span.end.index() > span.start.index();
                self.frames.push(Frame::Sequence {
                    items: Vec::new(),
                    flow,
                });
            }
            Event::MappingEnd => {
                if let Some(Frame::Mapping { entries, .. }) = self.frames.pop() {
                    self.complete(Node::Mapping(entries));
                }
            }
            Event::SequenceEnd => {
                if let Some(Frame::Sequence { items, .. }) = self.frames.pop() {
                    self.complete(Node::Sequence(items));
                }
            }
            _ => {}
        }
    }
}
