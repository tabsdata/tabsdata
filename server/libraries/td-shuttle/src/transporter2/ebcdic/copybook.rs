//
// Copyright 2026 Tabsdata Inc.
//

//! COBOL copybook parser: text → a flat column layout with byte offsets.
//!
//! Fixed-format input: columns 1–6 and 73–80 are discarded, so a free-format
//! copybook fails rather than losing its level numbers silently.
//!
//! An unmodelled clause is a hard error, never a warning: a missed storage
//! effect shifts every later offset, yielding plausible garbage rather than a
//! crash. Add it to [`clause_of`]; do not relax the error.

use crate::transporter2::error::{Result, TransporterError};
use std::collections::{HashMap, HashSet};

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(TransporterError::Copybook(msg.into()))
}

// --------------------------------------------------------------------------
// Model
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Usage {
    Display,
    /// COMP-3, PACKED-DECIMAL.
    Packed,
    /// COMP, COMP-4, COMP-5, BINARY.
    Binary,
    /// COMP-1.
    Float4,
    /// COMP-2.
    Float8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignPos {
    /// Overpunched into the last digit byte (the default).
    Overpunch,
    /// One extra byte in front.
    LeadingSeparate,
    /// One extra byte at the end.
    TrailingSeparate,
}

/// What a column holds and how to read its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Text,
    Zoned {
        scale: u32,
        signed: bool,
        sign: SignPos,
    },
    Packed {
        scale: u32,
    },
    Binary {
        scale: u32,
        signed: bool,
    },
    /// Numeric-edited: read as characters, strip the edit symbols.
    Edited {
        scale: u32,
    },
    Float {
        double: bool,
        hfp: bool,
    },
}

/// `&record[offset .. offset + len]` is always in bounds for one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    /// Uppercased, with OCCURS suffixes and de-duplication applied.
    pub name: String,
    pub offset: usize,
    pub len: usize,
    /// Only meaningful for the numeric kinds.
    pub digits: u32,
    pub kind: Kind,
}

/// A parsed copybook: the columns and the record length they span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    pub cols: Vec<Col>,
    pub lrecl: usize,
}

// --------------------------------------------------------------------------
// PIC parsing
// --------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Pic {
    Alnum {
        len: u32,
    },
    Num {
        digits: u32,
        scale: u32,
        signed: bool,
    },
    Edited {
        len: u32,
        digits: u32,
        scale: u32,
    },
}

/// Expand repeat factors: `9(5)` → `99999`, `X(3)` → `XXX`.
fn expand_pic(pic: &str) -> Result<String> {
    let chars: Vec<char> = pic.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '(' {
            let Some(rel) = chars[i..].iter().position(|&x| x == ')') else {
                return err(format!("unclosed ( in PIC {pic}"));
            };
            let close = i + rel;
            let count: String = chars[i + 1..close].iter().collect();
            let Ok(n) = count.trim().parse::<usize>() else {
                return err(format!("bad repeat count in PIC {pic}"));
            };
            let Some(last) = out.chars().last() else {
                return err(format!("PIC {pic} starts with ("));
            };
            if n == 0 {
                return err(format!("PIC {pic} has a zero repeat count"));
            }
            for _ in 1..n {
                out.push(last);
            }
            i = close + 1;
        } else {
            out.push(c);
            i += 1;
        }
    }
    Ok(out)
}

const EDIT_CHARS: &[char] = &[
    'Z', '*', '$', ',', '.', '+', '-', '/', 'B', '0', 'C', 'R', 'D',
];

/// Every picture character this module models; anything else is refused.
const KNOWN_PIC_CHARS: &[char] = &[
    '9', 'X', 'A', 'S', 'V', 'Z', '*', '$', ',', '.', '+', '-', '/', 'B', '0', 'C', 'R', 'D',
];

fn parse_pic(pic: &str) -> Result<Pic> {
    let e = expand_pic(&pic.to_uppercase())?;

    for c in e.chars() {
        match c {
            // P shifts the decimal point without occupying a byte: ignoring
            // it keeps offsets right and makes every value wrong by 10^n.
            'P' => {
                return err(format!(
                    "PIC {pic}: scaling positions (P) are not supported"
                ));
            }
            'G' | 'N' => {
                return err(format!("PIC {pic}: DBCS pictures are not supported"));
            }
            c if !KNOWN_PIC_CHARS.contains(&c) => {
                return err(format!("PIC {pic}: unsupported picture character `{c}`"));
            }
            _ => {}
        }
    }

    let has_edit = e.chars().any(|c| EDIT_CHARS.contains(&c));
    let has_alpha = e.contains('X') || e.contains('A');

    if has_edit && !has_alpha {
        // Numeric-edited. Every character except S and V occupies a byte.
        let len = e.chars().filter(|&c| c != 'S' && c != 'V').count() as u32;
        let digit_positions: Vec<usize> = e
            .char_indices()
            .filter(|(_, c)| matches!(c, '9' | 'Z' | '*'))
            .map(|(i, _)| i)
            .collect();
        let digits = digit_positions.len() as u32;
        if digits == 0 {
            return err(format!("PIC {pic} has no digit positions"));
        }
        let point = e.find('.').or_else(|| e.find('V'));
        let scale = match point {
            Some(p) => digit_positions.iter().filter(|&&i| i > p).count() as u32,
            None => 0,
        };
        return Ok(Pic::Edited { len, digits, scale });
    }

    if has_alpha {
        // Alphanumeric / alphabetic. X, A and 9 each take one byte.
        let len = e.chars().filter(|&c| matches!(c, 'X' | 'A' | '9')).count() as u32;
        if len == 0 {
            return err(format!("could not size PIC {pic}"));
        }
        return Ok(Pic::Alnum { len });
    }

    let signed = e.starts_with('S');
    let digits = e.chars().filter(|&c| c == '9').count() as u32;
    let scale = match e.find('V') {
        Some(p) => e[p..].chars().filter(|&c| c == '9').count() as u32,
        None => 0,
    };
    if digits == 0 {
        return err(format!("could not size PIC {pic}"));
    }
    Ok(Pic::Num {
        digits,
        scale,
        signed,
    })
}

// --------------------------------------------------------------------------
// Statement parsing
// --------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Stmt {
    /// Level 77 is normalised to 1 — it can never be subordinate.
    level: u8,
    name: String,
    redefines: Option<String>,
    pic: Option<Pic>,
    usage: Usage,
    occurs: u32,
    sign: SignPos,
    sync: bool,
}

/// Drop columns 1–6 and 73–80. Character-indexed, not byte-indexed: a stray
/// multi-byte character must not panic.
fn strip_columns(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    if chars.len() <= 6 {
        return String::new();
    }
    // 6 is the indicator column; 7..72 are areas A and B.
    let end = chars.len().min(72);
    chars[6..end].iter().collect()
}

/// Split on statement-terminating periods: a period ends a statement only when
/// followed by whitespace or end of input, which is what lets `PIC ZZ,ZZ9.99.`
/// parse. Quoted strings are tracked so `VALUE 'A.B'` does not split.
fn split_statements(text: &str) -> Vec<String> {
    let mut body = String::new();
    for line in text.lines() {
        let cut = strip_columns(line);
        let trimmed = cut.trim_start();
        if trimmed.starts_with('*') || trimmed.starts_with('/') {
            continue;
        }
        body.push(' ');
        body.push_str(&cut);
    }

    let chars: Vec<char> = body.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for (i, &c) in chars.iter().enumerate() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                    cur.push(c);
                } else if c == '.' && chars.get(i + 1).is_none_or(|n| n.is_whitespace()) {
                    if !cur.trim().is_empty() {
                        out.push(cur.trim().to_string());
                    }
                    cur.clear();
                } else {
                    cur.push(c);
                }
            }
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn tokenize(stmt: &str) -> Vec<String> {
    let mut toks = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in stmt.chars() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if c == '\'' || c == '"' => {
                quote = Some(c);
                cur.push(c);
            }
            None if c.is_whitespace() => {
                if !cur.is_empty() {
                    toks.push(std::mem::take(&mut cur));
                }
            }
            None => cur.push(c),
        }
    }
    if !cur.is_empty() {
        toks.push(cur);
    }
    toks
}

/// The clause a token starts, or `None` if it is not a keyword.
///
/// The single source of truth: the parser dispatches on it *and* `VALUE`'s
/// skip loop stops at it, so a usage clause after a literal cannot be
/// swallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Clause {
    Redefines,
    Pic,
    Occurs,
    Sign,
    Sync,
    Usage,
    Packed,
    Float4,
    Float8,
    Binary,
    Display,
    Value,
    Blank,
    /// Introduces a list of user-supplied names (`ASCENDING KEY IS A B`,
    /// `INDEXED BY I J`). Storage-neutral, but the operands are identifiers,
    /// so they cannot be told apart from an unknown keyword — inside a name
    /// list, and only there, the refusal policy cannot apply.
    NameList,
    /// Recognised, no storage effect.
    Ignored,
}

fn clause_of(tok: &str) -> Option<Clause> {
    Some(match tok {
        "REDEFINES" => Clause::Redefines,
        "PIC" | "PICTURE" => Clause::Pic,
        "OCCURS" => Clause::Occurs,
        "SIGN" => Clause::Sign,
        "SYNC" | "SYNCHRONIZED" => Clause::Sync,
        "USAGE" => Clause::Usage,
        "COMP-3" | "COMPUTATIONAL-3" | "PACKED-DECIMAL" => Clause::Packed,
        "COMP-1" | "COMPUTATIONAL-1" => Clause::Float4,
        "COMP-2" | "COMPUTATIONAL-2" => Clause::Float8,
        "COMP" | "COMP-4" | "COMP-5" | "COMPUTATIONAL" | "COMPUTATIONAL-4" | "COMPUTATIONAL-5"
        | "BINARY" => Clause::Binary,
        "DISPLAY" => Clause::Display,
        "VALUE" | "VALUES" => Clause::Value,
        "BLANK" => Clause::Blank,
        "ASCENDING" | "DESCENDING" | "INDEXED" => Clause::NameList,
        "JUST" | "JUSTIFIED" | "RIGHT" | "LEFT" | "GLOBAL" | "EXTERNAL" | "KEY" | "BY" | "ON"
        | "IS" => Clause::Ignored,
        _ => return None,
    })
}

/// `Ok(None)` means the statement occupies no storage and is skipped.
fn parse_stmt(stmt: &str) -> Result<Option<Stmt>> {
    let toks = tokenize(stmt);
    if toks.len() < 2 {
        return Ok(None);
    }
    let Ok(raw_level) = toks[0].parse::<u8>() else {
        return Ok(None); // not a data description entry
    };
    // 88 = condition name, 66 = RENAMES: neither occupies storage.
    if raw_level == 88 || raw_level == 66 {
        return Ok(None);
    }

    let name = toks[1].to_uppercase();
    let mut s = Stmt {
        level: if raw_level == 77 { 1 } else { raw_level },
        name: name.clone(),
        redefines: None,
        pic: None,
        usage: Usage::Display,
        occurs: 1,
        sign: SignPos::Overpunch,
        sync: false,
    };

    let up: Vec<String> = toks.iter().map(|t| t.to_uppercase()).collect();
    let mut i = 2;
    while i < up.len() {
        let t = up[i].as_str();
        let Some(clause) = clause_of(t) else {
            return err(format!(
                "{name}: unrecognized clause `{t}` — refusing to guess the layout"
            ));
        };
        match clause {
            Clause::Redefines => {
                let Some(target) = up.get(i + 1) else {
                    return err(format!("{name}: REDEFINES needs a target"));
                };
                s.redefines = Some(target.clone());
                i += 2;
            }
            Clause::Pic => {
                let mut j = i + 1;
                if up.get(j).map(String::as_str) == Some("IS") {
                    j += 1;
                }
                let Some(p) = up.get(j) else {
                    return err(format!("{name}: PIC with no picture string"));
                };
                s.pic = Some(parse_pic(p)?);
                i = j + 1;
            }
            Clause::Occurs => {
                let mut j = i + 1;
                let Some(n) = up.get(j).and_then(|v| v.parse::<u32>().ok()) else {
                    return err(format!("{name}: OCCURS needs a count"));
                };
                j += 1;
                if up.get(j).map(String::as_str) == Some("TO") {
                    return err(format!(
                        "{name}: OCCURS DEPENDING ON makes records variable-length; \
                         not supported"
                    ));
                }
                if up.get(j).map(String::as_str) == Some("TIMES") {
                    j += 1;
                }
                if up.get(j).map(String::as_str) == Some("DEPENDING") {
                    return err(format!(
                        "{name}: OCCURS DEPENDING ON makes records variable-length; \
                         not supported"
                    ));
                }
                if n == 0 {
                    return err(format!("{name}: OCCURS 0 is not a table"));
                }
                s.occurs = n;
                i = j;
            }
            Clause::Sign => {
                let mut j = i + 1;
                if up.get(j).map(String::as_str) == Some("IS") {
                    j += 1;
                }
                let pos = up.get(j).map(String::as_str);
                j += 1;
                if up.get(j).map(String::as_str) == Some("SEPARATE") {
                    j += 1;
                    if up.get(j).map(String::as_str) == Some("CHARACTER") {
                        j += 1;
                    }
                    s.sign = match pos {
                        Some("LEADING") => SignPos::LeadingSeparate,
                        Some("TRAILING") => SignPos::TrailingSeparate,
                        _ => return err(format!("{name}: bad SIGN clause")),
                    };
                }
                i = j;
            }
            Clause::Sync => {
                s.sync = true;
                i += 1;
                if matches!(up.get(i).map(String::as_str), Some("LEFT") | Some("RIGHT")) {
                    i += 1;
                }
            }
            Clause::Usage => {
                i += 1;
                if up.get(i).map(String::as_str) == Some("IS") {
                    i += 1;
                }
            }
            Clause::Packed => {
                s.usage = Usage::Packed;
                i += 1;
            }
            Clause::Float4 => {
                s.usage = Usage::Float4;
                i += 1;
            }
            Clause::Float8 => {
                s.usage = Usage::Float8;
                i += 1;
            }
            Clause::Binary => {
                s.usage = Usage::Binary;
                i += 1;
            }
            Clause::Display => {
                s.usage = Usage::Display;
                i += 1;
            }
            Clause::Value => {
                i += 1;
                while i < up.len() && clause_of(&up[i]).is_none() {
                    i += 1;
                }
            }
            Clause::Blank => {
                i += 1;
                while matches!(up.get(i).map(String::as_str), Some("WHEN") | Some("ZERO")) {
                    i += 1;
                }
            }
            Clause::NameList => {
                i += 1;
                while i < up.len() {
                    // `KEY`, `BY` and `IS` belong to this clause, so they do
                    // not end the name list the way another clause would.
                    if matches!(up[i].as_str(), "KEY" | "BY" | "IS") || clause_of(&up[i]).is_none()
                    {
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            Clause::Ignored => i += 1,
        }
    }
    Ok(Some(s))
}

// --------------------------------------------------------------------------
// Layout
// --------------------------------------------------------------------------

#[derive(Debug)]
struct Node {
    stmt: Stmt,
    children: Vec<Node>,
}

fn build_tree(stmts: Vec<Stmt>) -> Vec<Node> {
    let mut roots: Vec<Node> = Vec::new();
    let mut path: Vec<usize> = Vec::new();

    for s in stmts {
        let node = Node {
            stmt: s.clone(),
            children: Vec::new(),
        };
        while !path.is_empty() {
            if node_at(&roots, &path).stmt.level < s.level {
                break;
            }
            path.pop();
        }
        if path.is_empty() {
            roots.push(node);
            path.push(roots.len() - 1);
        } else {
            let parent = node_at_mut(&mut roots, &path);
            parent.children.push(node);
            let idx = parent.children.len() - 1;
            path.push(idx);
        }
    }
    roots
}

fn node_at<'a>(roots: &'a [Node], path: &[usize]) -> &'a Node {
    let mut n = &roots[path[0]];
    for &i in &path[1..] {
        n = &n.children[i];
    }
    n
}

fn node_at_mut<'a>(roots: &'a mut [Node], path: &[usize]) -> &'a mut Node {
    let mut n = &mut roots[path[0]];
    for &i in &path[1..] {
        n = &mut n.children[i];
    }
    n
}

/// COMP storage width. IBM allocates by declared digit count, not by value.
pub fn binary_width(digits: u32) -> Option<usize> {
    match digits {
        1..=4 => Some(2),
        5..=9 => Some(4),
        10..=18 => Some(8),
        _ => None,
    }
}

fn elem_len(s: &Stmt) -> Result<usize> {
    match s.usage {
        Usage::Float4 => return Ok(4),
        Usage::Float8 => return Ok(8),
        _ => {}
    }
    let Some(pic) = s.pic.as_ref() else {
        return err(format!("{}: elementary item with no PIC clause", s.name));
    };
    Ok(match (pic, s.usage) {
        (Pic::Alnum { len }, Usage::Display) => *len as usize,
        (Pic::Edited { len, .. }, Usage::Display) => *len as usize,
        (Pic::Num { digits, .. }, Usage::Display) => {
            *digits as usize + usize::from(s.sign != SignPos::Overpunch)
        }
        (Pic::Num { digits, .. }, Usage::Packed) => *digits as usize / 2 + 1,
        (Pic::Num { digits, .. }, Usage::Binary) => match binary_width(*digits) {
            Some(w) => w,
            None => return err(format!("{}: COMP needs 1-18 digits, got {digits}", s.name)),
        },
        (Pic::Alnum { .. } | Pic::Edited { .. }, usage) => {
            return err(format!(
                "{}: PIC X/edited cannot have usage {usage:?}",
                s.name
            ));
        }
        // Float usages returned above; this arm only satisfies exhaustiveness.
        (Pic::Num { .. }, Usage::Float4 | Usage::Float8) => {
            return err(format!("{}: COMP-1/COMP-2 take no PIC clause", s.name));
        }
    })
}

/// SYNCHRONIZED aligns binary and float items to their natural boundary.
fn sync_align(s: &Stmt, len: usize) -> usize {
    match s.usage {
        Usage::Binary | Usage::Float4 | Usage::Float8 => len.clamp(1, 8),
        _ => 1,
    }
}

fn elem_kind(s: &Stmt, hfp: bool) -> Result<(Kind, u32)> {
    match s.usage {
        Usage::Float4 => return Ok((Kind::Float { double: false, hfp }, 0)),
        Usage::Float8 => return Ok((Kind::Float { double: true, hfp }, 0)),
        _ => {}
    }
    let Some(pic) = s.pic.as_ref() else {
        return err(format!("{}: elementary item with no PIC clause", s.name));
    };
    Ok(match (pic, s.usage) {
        (Pic::Alnum { len }, _) => (Kind::Text, *len),
        (Pic::Edited { digits, scale, .. }, _) => (Kind::Edited { scale: *scale }, *digits),
        (
            Pic::Num {
                digits,
                scale,
                signed,
            },
            Usage::Display,
        ) => (
            Kind::Zoned {
                scale: *scale,
                signed: *signed,
                sign: s.sign,
            },
            *digits,
        ),
        (Pic::Num { digits, scale, .. }, Usage::Packed) => {
            (Kind::Packed { scale: *scale }, *digits)
        }
        (
            Pic::Num {
                digits,
                scale,
                signed,
            },
            Usage::Binary,
        ) => (
            Kind::Binary {
                scale: *scale,
                signed: *signed,
            },
            *digits,
        ),
        (Pic::Num { .. }, Usage::Float4 | Usage::Float8) => {
            return err(format!("{}: COMP-1/COMP-2 take no PIC clause", s.name));
        }
    })
}

struct Ctx {
    cols: Vec<Col>,
    keep_filler: bool,
    hfp: bool,
}

/// Lay out one group's members. `scope` is local to the group because
/// `REDEFINES` names a preceding item *in the same group*; a copybook-wide
/// name map lets two groups sharing a field name rewind to the wrong offset.
fn lay_siblings(nodes: &[Node], cursor: &mut usize, suffix: &str, ctx: &mut Ctx) -> Result<()> {
    let mut scope: HashMap<String, usize> = HashMap::new();
    for n in nodes {
        lay(n, cursor, suffix, &mut scope, ctx)?;
    }
    Ok(())
}

fn lay(
    node: &Node,
    cursor: &mut usize,
    suffix: &str,
    scope: &mut HashMap<String, usize>,
    ctx: &mut Ctx,
) -> Result<()> {
    let s = &node.stmt;
    let saved_cursor = *cursor;
    let elementary = node.children.is_empty();

    if let Some(target) = &s.redefines {
        if s.occurs > 1 {
            return err(format!(
                "{}: REDEFINES combined with OCCURS is not supported",
                s.name
            ));
        }
        let Some(base) = scope.get(target).copied() else {
            return err(format!(
                "{}: REDEFINES unknown field {target} — the target must be a preceding \
                 item in the same group",
                s.name
            ));
        };
        *cursor = base;
    }

    // Align before recording, so a REDEFINES onto a synchronised item rewinds
    // to the aligned offset. The round-up in the loop is then a first-pass no-op.
    if elementary && s.sync {
        let a = sync_align(s, elem_len(s)?);
        *cursor = cursor.div_ceil(a) * a;
    }
    scope.insert(s.name.clone(), *cursor);

    for idx in 1..=s.occurs {
        let sfx = if s.occurs > 1 {
            format!("{suffix}_{idx}")
        } else {
            suffix.to_string()
        };

        if elementary {
            let len = elem_len(s)?;
            if s.sync {
                let a = sync_align(s, len);
                *cursor = cursor.div_ceil(a) * a;
            }
            if ctx.keep_filler || s.name != "FILLER" {
                let (kind, digits) = elem_kind(s, ctx.hfp)?;
                ctx.cols.push(Col {
                    name: format!("{}{}", s.name, sfx),
                    offset: *cursor,
                    len,
                    digits,
                    kind,
                });
            }
            *cursor += len;
        } else {
            lay_siblings(&node.children, cursor, &sfx, ctx)?;
        }
    }

    if s.redefines.is_some() {
        // A redefinition may be longer than what it overlays; the record grows.
        *cursor = (*cursor).max(saved_cursor);
    }
    Ok(())
}

/// Make column names unique — arrow rejects duplicates, so this is not
/// cosmetic. Candidates the copybook itself declares are skipped.
fn dedupe(cols: &mut [Col]) {
    let declared: HashSet<String> = cols.iter().map(|c| c.name.clone()).collect();
    let mut used: HashSet<String> = HashSet::new();
    for c in cols.iter_mut() {
        if used.insert(c.name.clone()) {
            continue;
        }
        let mut n = 2usize;
        loop {
            let candidate = format!("{}_{n}", c.name);
            if !declared.contains(&candidate) && !used.contains(&candidate) {
                used.insert(candidate.clone());
                c.name = candidate;
                break;
            }
            n += 1;
        }
    }
}

/// Parse `text` into a column layout. `keep_filler` emits `FILLER` items as
/// columns (they advance the cursor either way); `hfp` reads `COMP-1`/`COMP-2`
/// as IBM hex float rather than IEEE 754.
pub fn parse_copybook(text: &str, keep_filler: bool, hfp: bool) -> Result<Layout> {
    let mut stmts = Vec::new();
    for s in split_statements(text) {
        if let Some(st) = parse_stmt(&s)? {
            stmts.push(st);
        }
    }
    if stmts.is_empty() {
        return err(
            "no data description entries found in copybook — the parser expects \
             fixed-format COBOL with code starting at column 8",
        );
    }

    let roots = build_tree(stmts);
    // Laying several independent layouts in sequence would sum their lengths
    // into one oversized record. REDEFINES alternatives rewind, so they are fine.
    if let Some(extra) = roots
        .iter()
        .skip(1)
        .find(|r| r.stmt.redefines.is_none())
        .map(|r| r.stmt.name.clone())
    {
        return err(format!(
            "copybook declares more than one record layout ({} and {extra}); \
             supply a copybook with a single top-level record",
            roots[0].stmt.name
        ));
    }
    if let Some(top) = roots.iter().find(|r| r.stmt.occurs > 1) {
        return err(format!("{}: a record layout cannot OCCURS", top.stmt.name));
    }

    let mut ctx = Ctx {
        cols: Vec::new(),
        keep_filler,
        hfp,
    };
    let mut cursor = 0usize;
    lay_siblings(&roots, &mut cursor, "", &mut ctx)?;

    let mut cols = ctx.cols;
    dedupe(&mut cols);
    if cols.is_empty() {
        return err("copybook produced no columns");
    }
    Ok(Layout {
        cols,
        lrecl: cursor,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    // --- fixtures -------------------------------------------------------

    /// A copybook in **fixed format**: six sequence columns then the code.
    /// Every test goes through this, because forgetting the six columns is the
    /// single most common way to hand this parser something it cannot read.
    fn book(lines: &[&str]) -> String {
        lines
            .iter()
            .map(|l| format!("      {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn parse(lines: &[&str]) -> Layout {
        parse_copybook(&book(lines), false, false).expect("copybook should parse")
    }

    fn parse_err(lines: &[&str]) -> String {
        match parse_copybook(&book(lines), false, false) {
            Ok(layout) => panic!("expected a parse error, got {layout:?}"),
            Err(e) => e.to_string(),
        }
    }

    /// `(name, offset, len)` for every column — the layout contract, without
    /// the type noise.
    fn spans(layout: &Layout) -> Vec<(&str, usize, usize)> {
        layout
            .cols
            .iter()
            .map(|c| (c.name.as_str(), c.offset, c.len))
            .collect()
    }

    fn names(layout: &Layout) -> Vec<&str> {
        layout.cols.iter().map(|c| c.name.as_str()).collect()
    }

    fn kind_of(layout: &Layout, name: &str) -> Kind {
        layout
            .cols
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no column {name} in {:?}", names(layout)))
            .kind
    }

    // --- the basic contract ---------------------------------------------

    #[test]
    fn columns_are_laid_out_end_to_end_and_lrecl_is_their_sum() {
        let layout = parse(&[
            "01  REC.",
            "    05  ID      PIC 9(5).",
            "    05  NAME    PIC X(10).",
            "    05  BALANCE PIC S9(7)V99 COMP-3.",
        ]);
        assert_eq!(
            spans(&layout),
            vec![("ID", 0, 5), ("NAME", 5, 10), ("BALANCE", 15, 5)]
        );
        assert_eq!(layout.lrecl, 20);
    }

    #[test]
    fn every_field_slice_is_in_bounds_for_the_record_length() {
        let layout = parse(&[
            "01  REC.",
            "    05  A PIC X(3).",
            "    05  B PIC 9(4) COMP.",
            "    05  C PIC S9(3) COMP-3.",
        ]);
        for c in &layout.cols {
            assert!(
                c.offset + c.len <= layout.lrecl,
                "{} runs past the record: {}+{} > {}",
                c.name,
                c.offset,
                c.len,
                layout.lrecl
            );
        }
    }

    #[test]
    fn names_are_uppercased() {
        let layout = parse(&["01  rec.", "    05  cust-id PIC 9(3)."]);
        assert_eq!(names(&layout), vec!["CUST-ID"]);
    }

    #[test]
    fn a_group_contributes_no_storage_of_its_own() {
        let layout = parse(&[
            "01  REC.",
            "    05  ADDRESS.",
            "        10  STREET PIC X(4).",
            "        10  CITY   PIC X(4).",
            "    05  ZIP PIC X(2).",
        ]);
        assert_eq!(
            spans(&layout),
            vec![("STREET", 0, 4), ("CITY", 4, 4), ("ZIP", 8, 2)]
        );
        assert_eq!(layout.lrecl, 10);
    }

    #[test]
    fn level_numbers_need_not_be_contiguous() {
        let stepped = parse(&[
            "01  REC.",
            "    05  G.",
            "        10  A PIC X(2).",
            "    05  B PIC X(3).",
        ]);
        let dense = parse(&[
            "01  REC.",
            "    02  G.",
            "        03  A PIC X(2).",
            "    02  B PIC X(3).",
        ]);
        assert_eq!(spans(&stepped), spans(&dense));
    }

    // --- PIC sizing -------------------------------------------------------

    #[test]
    fn repeat_factors_expand() {
        let layout = parse(&[
            "01  REC.",
            "    05  A PIC X(3).",
            "    05  B PIC XXX.",
            "    05  C PIC 9(2)V9(3).",
        ]);
        assert_eq!(spans(&layout), vec![("A", 0, 3), ("B", 3, 3), ("C", 6, 5)]);
    }

    #[test]
    fn s_and_v_occupy_no_byte() {
        let layout = parse(&["01  REC.", "    05  A PIC S9(3)V99."]);
        assert_eq!(layout.lrecl, 5);
        assert_eq!(
            kind_of(&layout, "A"),
            Kind::Zoned {
                scale: 2,
                signed: true,
                sign: SignPos::Overpunch
            }
        );
    }

    #[test]
    fn numeric_edited_sizes_to_its_character_count() {
        // The documented case: nine characters, seven digit positions, scale 2.
        let layout = parse(&["01  REC.", "    05  AMT PIC ZZ,ZZ9.99."]);
        assert_eq!(layout.lrecl, 9);
        assert_eq!(kind_of(&layout, "AMT"), Kind::Edited { scale: 2 });
        assert_eq!(layout.cols[0].digits, 7);
    }

    #[test]
    fn an_edited_pic_with_no_decimal_point_has_scale_zero() {
        let layout = parse(&["01  REC.", "    05  AMT PIC ZZZ9."]);
        assert_eq!(kind_of(&layout, "AMT"), Kind::Edited { scale: 0 });
        assert_eq!(layout.lrecl, 4);
    }

    #[test]
    fn alphanumeric_counts_x_a_and_9() {
        let layout = parse(&["01  REC.", "    05  A PIC XX99AA."]);
        assert_eq!(layout.lrecl, 6);
        assert_eq!(kind_of(&layout, "A"), Kind::Text);
        // Text reports its byte count as its digit count.
        assert_eq!(layout.cols[0].digits, 6);
    }

    #[test]
    fn picture_is_case_insensitive() {
        let lower = parse(&["01  REC.", "    05  A pic s9(3)v99 comp-3."]);
        let upper = parse(&["01  REC.", "    05  A PIC S9(3)V99 COMP-3."]);
        assert_eq!(lower, upper);
    }

    #[test]
    fn picture_keyword_accepts_the_long_form_and_is() {
        let short = parse(&["01  REC.", "    05  A PIC X(4)."]);
        let long = parse(&["01  REC.", "    05  A PICTURE IS X(4)."]);
        assert_eq!(short, long);
    }

    // --- usage sizing -----------------------------------------------------

    #[test]
    fn packed_decimal_takes_half_a_byte_per_digit_plus_the_sign_nibble() {
        // Odd and even digit counts both round to digits/2 + 1.
        for (digits, expected) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (18, 10)] {
            let layout = parse(&["01  REC.", &format!("    05  A PIC S9({digits}) COMP-3.")]);
            assert_eq!(layout.lrecl, expected, "COMP-3 with {digits} digits");
        }
    }

    #[test]
    fn binary_width_follows_the_declared_digit_count_not_the_value() {
        for (digits, expected) in [(1, 2), (4, 2), (5, 4), (9, 4), (10, 8), (18, 8)] {
            let layout = parse(&["01  REC.", &format!("    05  A PIC S9({digits}) COMP.")]);
            assert_eq!(layout.lrecl, expected, "COMP with {digits} digits");
        }
        assert_eq!(binary_width(19), None);
    }

    #[test]
    fn every_usage_synonym_maps_to_the_same_storage() {
        let cases = [
            (vec!["COMP-3", "COMPUTATIONAL-3", "PACKED-DECIMAL"], 3usize),
            (
                vec![
                    "COMP",
                    "COMP-4",
                    "COMP-5",
                    "COMPUTATIONAL",
                    "COMPUTATIONAL-4",
                    "COMPUTATIONAL-5",
                    "BINARY",
                ],
                4,
            ),
        ];
        for (synonyms, expected) in cases {
            for word in synonyms {
                let layout = parse(&["01  REC.", &format!("    05  A PIC S9(5) {word}.")]);
                assert_eq!(layout.lrecl, expected, "{word}");
            }
        }
    }

    #[test]
    fn usage_is_prefix_is_accepted() {
        let bare = parse(&["01  REC.", "    05  A PIC S9(5) COMP-3."]);
        let verbose = parse(&["01  REC.", "    05  A PIC S9(5) USAGE IS COMP-3."]);
        assert_eq!(bare, verbose);
    }

    #[test]
    fn floats_are_four_and_eight_bytes_and_take_no_pic() {
        let layout = parse(&["01  REC.", "    05  A COMP-1.", "    05  B COMP-2."]);
        assert_eq!(spans(&layout), vec![("A", 0, 4), ("B", 4, 8)]);
        assert_eq!(
            kind_of(&layout, "A"),
            Kind::Float {
                double: false,
                hfp: false
            }
        );
    }

    #[test]
    fn the_hfp_flag_reaches_the_float_kind() {
        let layout =
            parse_copybook(&book(&["01  REC.", "    05  A COMP-2."]), false, true).unwrap();
        assert_eq!(
            kind_of(&layout, "A"),
            Kind::Float {
                double: true,
                hfp: true
            }
        );
    }

    // --- SIGN -------------------------------------------------------------

    #[test]
    fn a_separate_sign_adds_a_byte_and_an_overpunched_one_does_not() {
        let overpunch = parse(&["01  REC.", "    05  A PIC S9(5)."]);
        let leading = parse(&["01  REC.", "    05  A PIC S9(5) SIGN LEADING SEPARATE."]);
        let trailing = parse(&[
            "01  REC.",
            "    05  A PIC S9(5) SIGN IS TRAILING SEPARATE CHARACTER.",
        ]);
        assert_eq!(overpunch.lrecl, 5);
        assert_eq!(leading.lrecl, 6);
        assert_eq!(trailing.lrecl, 6);
        assert_eq!(
            kind_of(&leading, "A"),
            Kind::Zoned {
                scale: 0,
                signed: true,
                sign: SignPos::LeadingSeparate
            }
        );
        assert_eq!(
            kind_of(&trailing, "A"),
            Kind::Zoned {
                scale: 0,
                signed: true,
                sign: SignPos::TrailingSeparate
            }
        );
    }

    #[test]
    fn sign_without_separate_leaves_the_sign_overpunched() {
        // `SIGN IS LEADING` on its own does not add a byte.
        let layout = parse(&["01  REC.", "    05  A PIC S9(5) SIGN IS LEADING."]);
        assert_eq!(layout.lrecl, 5);
        assert_eq!(
            kind_of(&layout, "A"),
            Kind::Zoned {
                scale: 0,
                signed: true,
                sign: SignPos::Overpunch
            }
        );
    }

    // --- SYNC -------------------------------------------------------------

    #[test]
    fn sync_rounds_a_binary_item_up_to_its_own_width() {
        let layout = parse(&[
            "01  REC.",
            "    05  PAD PIC X(3).",
            "    05  N   PIC S9(9) COMP SYNC.",
        ]);
        // 4-byte binary aligns to 4: offset 3 becomes 4.
        assert_eq!(spans(&layout), vec![("PAD", 0, 3), ("N", 4, 4)]);
        assert_eq!(layout.lrecl, 8);
    }

    #[test]
    fn sync_does_not_move_a_display_item() {
        let layout = parse(&[
            "01  REC.",
            "    05  PAD PIC X(3).",
            "    05  N   PIC 9(5) SYNC.",
        ]);
        assert_eq!(spans(&layout), vec![("PAD", 0, 3), ("N", 3, 5)]);
    }

    #[test]
    fn sync_accepts_the_left_and_right_qualifiers() {
        let plain = parse(&[
            "01  REC.",
            "    05  A PIC X.",
            "    05  N PIC S9(9) COMP SYNC.",
        ]);
        let right = parse(&[
            "01  REC.",
            "    05  A PIC X.",
            "    05  N PIC S9(9) COMP SYNCHRONIZED RIGHT.",
        ]);
        assert_eq!(spans(&plain), spans(&right));
    }

    /// Regression: the offset a REDEFINES rewinds to must be the *aligned* one.
    #[test]
    fn redefines_onto_a_synchronised_item_rewinds_to_the_aligned_offset() {
        let layout = parse(&[
            "01  REC.",
            "    05  PAD PIC X(3).",
            "    05  N   PIC S9(9) COMP SYNC.",
            "    05  N-C REDEFINES N PIC X(4).",
        ]);
        let n = layout.cols.iter().find(|c| c.name == "N").unwrap();
        let alt = layout.cols.iter().find(|c| c.name == "N-C").unwrap();
        assert_eq!(n.offset, 4);
        assert_eq!(
            alt.offset, 4,
            "the redefinition must overlay N, not the pad"
        );
    }

    // --- OCCURS -----------------------------------------------------------

    #[test]
    fn occurs_on_an_elementary_item_suffixes_the_index() {
        let layout = parse(&["01  REC.", "    05  SCORES PIC 9(3) OCCURS 3 TIMES."]);
        assert_eq!(
            spans(&layout),
            vec![("SCORES_1", 0, 3), ("SCORES_2", 3, 3), ("SCORES_3", 6, 3)]
        );
        assert_eq!(layout.lrecl, 9);
    }

    #[test]
    fn occurs_on_a_group_suffixes_every_descendant() {
        let layout = parse(&[
            "01  REC.",
            "    05  LINE OCCURS 2.",
            "        10  SKU PIC X(3).",
            "        10  QTY PIC 9(2).",
        ]);
        assert_eq!(
            spans(&layout),
            vec![
                ("SKU_1", 0, 3),
                ("QTY_1", 3, 2),
                ("SKU_2", 5, 3),
                ("QTY_2", 8, 2),
            ]
        );
    }

    #[test]
    fn nested_occurs_accumulate_suffixes() {
        let layout = parse(&[
            "01  REC.",
            "    05  OUTER OCCURS 2.",
            "        10  INNER PIC X OCCURS 2.",
        ]);
        assert_eq!(
            names(&layout),
            vec!["INNER_1_1", "INNER_1_2", "INNER_2_1", "INNER_2_2"]
        );
    }

    #[test]
    fn occurs_1_adds_no_suffix() {
        let layout = parse(&["01  REC.", "    05  A PIC X OCCURS 1 TIMES."]);
        assert_eq!(names(&layout), vec!["A"]);
    }

    // --- REDEFINES --------------------------------------------------------

    #[test]
    fn a_shorter_redefinition_does_not_shrink_the_record() {
        let layout = parse(&[
            "01  REC.",
            "    05  A     PIC X(10).",
            "    05  A-ALT REDEFINES A PIC X(4).",
            "    05  B     PIC X(2).",
        ]);
        assert_eq!(
            spans(&layout),
            vec![("A", 0, 10), ("A-ALT", 0, 4), ("B", 10, 2)]
        );
        assert_eq!(layout.lrecl, 12);
    }

    #[test]
    fn a_longer_redefinition_grows_the_record() {
        let layout = parse(&[
            "01  REC.",
            "    05  A     PIC X(4).",
            "    05  A-ALT REDEFINES A PIC X(10).",
            "    05  B     PIC X(2).",
        ]);
        assert_eq!(
            spans(&layout),
            vec![("A", 0, 4), ("A-ALT", 0, 10), ("B", 10, 2)]
        );
        assert_eq!(layout.lrecl, 12);
    }

    #[test]
    fn a_group_can_be_redefined() {
        let layout = parse(&[
            "01  REC.",
            "    05  DATE-G.",
            "        10  YY PIC 9(4).",
            "        10  MM PIC 9(2).",
            "    05  DATE-C REDEFINES DATE-G PIC X(6).",
        ]);
        assert_eq!(
            spans(&layout),
            vec![("YY", 0, 4), ("MM", 4, 2), ("DATE-C", 0, 6)]
        );
        assert_eq!(layout.lrecl, 6);
    }

    /// Regression: `REDEFINES` names a preceding item **in the same group**.
    /// Resolving through one copybook-wide name map let a repeated field name
    /// in another group silently supply the offset.
    #[test]
    fn redefines_resolves_against_siblings_not_the_whole_copybook() {
        let layout = parse(&[
            "01  REC.",
            "    05  FIRST-G.",
            "        10  AMOUNT PIC X(9).",
            "    05  SECOND-G.",
            "        10  PAD    PIC X(2).",
            "        10  AMOUNT PIC X(4).",
            "        10  ALT    REDEFINES AMOUNT PIC X(4).",
        ]);
        // ALT must overlay the *second* AMOUNT at offset 11, not the first at 0.
        let alt = layout.cols.iter().find(|c| c.name == "ALT").unwrap();
        assert_eq!(alt.offset, 11);
    }

    #[test]
    fn redefining_a_name_from_another_group_is_an_error() {
        let message = parse_err(&[
            "01  REC.",
            "    05  FIRST-G.",
            "        10  AMOUNT PIC X(9).",
            "    05  SECOND-G.",
            "        10  ALT REDEFINES AMOUNT PIC X(4).",
        ]);
        assert!(
            message.contains("REDEFINES unknown field AMOUNT"),
            "{message}"
        );
        assert!(message.contains("same group"), "{message}");
    }

    #[test]
    fn redefining_an_unknown_field_is_an_error() {
        let message = parse_err(&["01  REC.", "    05  A REDEFINES NOPE PIC X(4)."]);
        assert!(
            message.contains("REDEFINES unknown field NOPE"),
            "{message}"
        );
    }

    /// Regression: the rewind happens once, outside the OCCURS loop, so an
    /// occurring redefinition would lay copies 2..n past its target.
    #[test]
    fn redefines_combined_with_occurs_is_refused() {
        let message = parse_err(&[
            "01  REC.",
            "    05  A PIC X(9).",
            "    05  B REDEFINES A PIC X(3) OCCURS 3.",
        ]);
        assert!(
            message.contains("REDEFINES combined with OCCURS"),
            "{message}"
        );
    }

    // --- FILLER -----------------------------------------------------------

    #[test]
    fn filler_advances_the_cursor_whether_or_not_it_is_kept() {
        let lines = [
            "01  REC.",
            "    05  A      PIC X(2).",
            "    05  FILLER PIC X(5).",
            "    05  B      PIC X(3).",
        ];
        let skipped = parse_copybook(&book(&lines), false, false).unwrap();
        let kept = parse_copybook(&book(&lines), true, false).unwrap();

        assert_eq!(spans(&skipped), vec![("A", 0, 2), ("B", 7, 3)]);
        assert_eq!(
            spans(&kept),
            vec![("A", 0, 2), ("FILLER", 2, 5), ("B", 7, 3)]
        );
        assert_eq!(skipped.lrecl, kept.lrecl);
    }

    #[test]
    fn repeated_filler_is_de_duplicated_when_kept() {
        let layout = parse_copybook(
            &book(&[
                "01  REC.",
                "    05  FILLER PIC X(2).",
                "    05  FILLER PIC X(3).",
            ]),
            true,
            false,
        )
        .unwrap();
        assert_eq!(names(&layout), vec!["FILLER", "FILLER_2"]);
    }

    // --- levels that occupy no storage ------------------------------------

    #[test]
    fn condition_names_and_renames_are_skipped() {
        let layout = parse(&[
            "01  REC.",
            "    05  FLAG PIC X.",
            "        88  FLAG-YES VALUE 'Y'.",
            "        88  FLAG-NO  VALUE 'N'.",
            "    05  B PIC X(2).",
            "    66  ALIAS RENAMES FLAG THRU B.",
        ]);
        assert_eq!(spans(&layout), vec![("FLAG", 0, 1), ("B", 1, 2)]);
        assert_eq!(layout.lrecl, 3);
    }

    /// Regression: level 77 is an *independent* item. Ordering purely by level
    /// number made `77 > 49`, so it nested under whatever preceded it.
    #[test]
    fn level_77_is_a_top_level_item_not_a_deep_child() {
        let message = parse_err(&["01  REC.", "    05  A PIC X(3).", "77  COUNTER PIC 9(3)."]);
        assert!(
            message.contains("more than one record layout"),
            "a 77 must not become a member of REC: {message}"
        );
    }

    #[test]
    fn a_lone_level_77_is_a_record_of_its_own() {
        let layout = parse(&["77  COUNTER PIC 9(3)."]);
        assert_eq!(spans(&layout), vec![("COUNTER", 0, 3)]);
    }

    // --- source-format handling -------------------------------------------

    #[test]
    fn sequence_columns_are_discarded() {
        // Digits in columns 1-6 must not be read as level numbers.
        let text = "000100 01  REC.\n000200     05  A PIC X(4).\n";
        let layout = parse_copybook(text, false, false).unwrap();
        assert_eq!(spans(&layout), vec![("A", 0, 4)]);
    }

    #[test]
    fn comment_lines_are_skipped() {
        let layout = parse(&[
            "01  REC.",
            "*   05  GHOST PIC X(99).",
            "/   05  ALSO-GHOST PIC X(99).",
            "    05  A PIC X(4).",
        ]);
        assert_eq!(spans(&layout), vec![("A", 0, 4)]);
    }

    #[test]
    fn the_identification_area_is_discarded() {
        // Columns 73-80 carry a program id, not code.
        let padding = " ".repeat(72 - 6 - "01  REC.".len());
        let text = format!("      01  REC.{padding}IGNORED1\n      05  A PIC X(4).\n");
        let layout = parse_copybook(&text, false, false).unwrap();
        assert_eq!(spans(&layout), vec![("A", 0, 4)]);
    }

    #[test]
    fn a_statement_may_span_several_lines() {
        let layout = parse(&[
            "01  REC.",
            "    05  A",
            "        PIC S9(5)",
            "        COMP-3.",
            "    05  B PIC X(2).",
        ]);
        assert_eq!(spans(&layout), vec![("A", 0, 3), ("B", 3, 2)]);
    }

    #[test]
    fn only_a_period_followed_by_whitespace_terminates_a_statement() {
        // The period inside ZZ,ZZ9.99 is followed by a digit.
        let layout = parse(&["01  REC.", "    05  AMT PIC ZZ,ZZ9.99.", "    05  B PIC X."]);
        assert_eq!(spans(&layout), vec![("AMT", 0, 9), ("B", 9, 1)]);
    }

    #[test]
    fn a_period_inside_a_quoted_literal_does_not_split() {
        let layout = parse(&[
            "01  REC.",
            "    05  A PIC X(3) VALUE 'A.B'.",
            "    05  B PIC X(2).",
        ]);
        assert_eq!(spans(&layout), vec![("A", 0, 3), ("B", 3, 2)]);
    }

    /// Regression: the column strip used byte indices, so a multi-byte
    /// character straddling column 6 or 72 panicked the decoder.
    #[test]
    fn a_non_ascii_line_does_not_panic() {
        let text =
            "*  £€ a comment with multi-byte characters\n      01  REC.\n      05  A PIC X(4).\n";
        let layout = parse_copybook(text, false, false).unwrap();
        assert_eq!(spans(&layout), vec![("A", 0, 4)]);
    }

    #[test]
    fn a_short_line_is_harmless() {
        let text = "\n   \n      01  REC.\n      05  A PIC X.\n";
        assert_eq!(parse_copybook(text, false, false).unwrap().lrecl, 1);
    }

    #[test]
    fn free_format_input_is_reported_rather_than_silently_mangled() {
        let message = match parse_copybook("01 REC.\n05 A PIC X(4).\n", false, false) {
            Ok(l) => panic!("expected an error, got {l:?}"),
            Err(e) => e.to_string(),
        };
        assert!(message.contains("no data description entries"), "{message}");
        assert!(message.contains("column 8"), "{message}");
    }

    // --- the refusal policy ------------------------------------------------

    #[test]
    fn an_unrecognized_clause_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC X(4) WOBBLE."]);
        assert!(
            message.contains("unrecognized clause `WOBBLE`"),
            "{message}"
        );
        assert!(message.contains("refusing to guess"), "{message}");
    }

    /// Regression, and the reason `clause_of` is the one source of truth: the
    /// `VALUE` literal skip used a second, shorter keyword list that omitted
    /// the `COMPUTATIONAL-n` spellings. A usage clause after a literal was
    /// swallowed, sizing this field 5 bytes instead of 3 and shifting every
    /// offset after it.
    #[test]
    fn a_usage_clause_after_a_value_literal_is_not_swallowed() {
        for word in [
            "COMP-3",
            "COMPUTATIONAL-3",
            "PACKED-DECIMAL",
            "USAGE IS COMP-3",
        ] {
            let layout = parse(&[
                "01  REC.",
                &format!("    05  A PIC S9(5) VALUE ZERO {word}."),
                "    05  B PIC X(2).",
            ]);
            assert_eq!(
                spans(&layout),
                vec![("A", 0, 3), ("B", 3, 2)],
                "the {word} after VALUE must still apply"
            );
        }
    }

    #[test]
    fn a_value_literal_alone_is_skipped() {
        let layout = parse(&[
            "01  REC.",
            "    05  A PIC X(3) VALUE 'ABC'.",
            "    05  B PIC 9(2) VALUE ZERO.",
        ]);
        assert_eq!(spans(&layout), vec![("A", 0, 3), ("B", 3, 2)]);
    }

    #[test]
    fn layout_neutral_clauses_are_consumed() {
        let layout = parse(&[
            "01  REC.",
            "    05  A PIC X(4) JUSTIFIED RIGHT.",
            "    05  B PIC 9(3) BLANK WHEN ZERO.",
            "    05  T PIC X(2) OCCURS 2 ASCENDING KEY IS T INDEXED BY I.",
        ]);
        assert_eq!(
            spans(&layout),
            vec![("A", 0, 4), ("B", 4, 3), ("T_1", 7, 2), ("T_2", 9, 2)]
        );
    }

    #[test]
    fn occurs_depending_on_is_refused_in_both_spellings() {
        for clause in ["OCCURS 1 TO 9 DEPENDING ON N", "OCCURS 9 DEPENDING ON N"] {
            let message = parse_err(&["01  REC.", &format!("    05  A PIC X {clause}.")]);
            assert!(message.contains("variable-length"), "{clause}: {message}");
        }
    }

    /// Regression: `P` shifts the implied decimal point without occupying a
    /// byte, so ignoring it left every offset right and every value wrong by a
    /// power of ten.
    #[test]
    fn scaling_positions_are_refused_rather_than_ignored() {
        for pic in ["9(5)P(3)", "PPP9(3)"] {
            let message = parse_err(&["01  REC.", &format!("    05  A PIC {pic}.")]);
            assert!(message.contains("scaling positions"), "{pic}: {message}");
        }
    }

    #[test]
    fn dbcs_pictures_are_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC G(4)."]);
        assert!(message.contains("DBCS"), "{message}");
    }

    #[test]
    fn an_unmodelled_picture_character_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC 9(3)E99."]);
        assert!(
            message.contains("unsupported picture character"),
            "{message}"
        );
    }

    #[test]
    fn a_zero_repeat_count_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC X(0)."]);
        assert!(message.contains("zero repeat count"), "{message}");
    }

    #[test]
    fn a_malformed_repeat_count_is_refused() {
        assert!(parse_err(&["01  REC.", "    05  A PIC X(3."]).contains("unclosed"));
        assert!(parse_err(&["01  REC.", "    05  A PIC X(Q)."]).contains("bad repeat count"));
    }

    #[test]
    fn a_comp_field_wider_than_eighteen_digits_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC 9(19) COMP."]);
        assert!(message.contains("COMP needs 1-18 digits"), "{message}");
    }

    #[test]
    fn an_elementary_item_with_no_pic_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC X.", "    05  B."]);
        assert!(message.contains("elementary item with no PIC"), "{message}");
    }

    #[test]
    fn a_text_picture_cannot_carry_a_numeric_usage() {
        let message = parse_err(&["01  REC.", "    05  A PIC X(4) COMP-3."]);
        assert!(message.contains("cannot have usage"), "{message}");
    }

    #[test]
    fn an_empty_copybook_is_refused() {
        assert!(parse_err(&[]).contains("no data description entries"));
        assert!(parse_err(&["*  just a comment"]).contains("no data description entries"));
    }

    /// Regression: laying several independent records in sequence summed their
    /// lengths into one oversized LRECL.
    #[test]
    fn two_unrelated_record_layouts_are_refused() {
        let message = parse_err(&[
            "01  REC-A.",
            "    05  A PIC X(4).",
            "01  REC-B.",
            "    05  B PIC X(4).",
        ]);
        assert!(message.contains("more than one record layout"), "{message}");
        assert!(message.contains("REC-A"), "{message}");
        assert!(message.contains("REC-B"), "{message}");
    }

    #[test]
    fn a_second_record_that_redefines_the_first_is_allowed() {
        let layout = parse(&[
            "01  REC.",
            "    05  A PIC X(4).",
            "01  REC-ALT REDEFINES REC.",
            "    05  B PIC X(2).",
            "    05  C PIC X(2).",
        ]);
        assert_eq!(spans(&layout), vec![("A", 0, 4), ("B", 0, 2), ("C", 2, 2)]);
        assert_eq!(layout.lrecl, 4);
    }

    #[test]
    fn a_record_layout_cannot_occurs() {
        let message = parse_err(&["01  REC OCCURS 3.", "    05  A PIC X."]);
        assert!(message.contains("cannot OCCURS"), "{message}");
    }

    #[test]
    fn occurs_zero_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC X OCCURS 0."]);
        assert!(message.contains("OCCURS 0"), "{message}");
    }

    #[test]
    fn occurs_without_a_count_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC X OCCURS."]);
        assert!(message.contains("OCCURS needs a count"), "{message}");
    }

    #[test]
    fn redefines_without_a_target_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC X.", "    05  B REDEFINES."]);
        assert!(message.contains("REDEFINES needs a target"), "{message}");
    }

    #[test]
    fn a_bad_sign_clause_is_refused() {
        let message = parse_err(&["01  REC.", "    05  A PIC S9(3) SIGN IS SIDEWAYS SEPARATE."]);
        assert!(message.contains("bad SIGN clause"), "{message}");
    }

    // --- name de-duplication ----------------------------------------------

    #[test]
    fn repeated_names_are_made_unique() {
        let layout = parse(&[
            "01  REC.",
            "    05  G1.",
            "        10  AMOUNT PIC X(2).",
            "    05  G2.",
            "        10  AMOUNT PIC X(2).",
            "    05  G3.",
            "        10  AMOUNT PIC X(2).",
        ]);
        assert_eq!(names(&layout), vec!["AMOUNT", "AMOUNT_2", "AMOUNT_3"]);
    }

    /// Regression: the generated suffix must not collide with a name the
    /// copybook itself declares.
    #[test]
    fn a_generated_name_skips_one_the_copybook_already_uses() {
        let layout = parse(&[
            "01  REC.",
            "    05  G1.",
            "        10  AMOUNT   PIC X(2).",
            "    05  G2.",
            "        10  AMOUNT   PIC X(2).",
            "    05  AMOUNT-2 PIC X(2).",
            "    05  G3.",
            "        10  AMOUNT   PIC X(2).",
        ]);
        assert_eq!(
            names(&layout),
            vec!["AMOUNT", "AMOUNT_2", "AMOUNT-2", "AMOUNT_3"]
        );
        // Whatever the renaming, every name is distinct.
        let unique: HashSet<&str> = names(&layout).into_iter().collect();
        assert_eq!(unique.len(), layout.cols.len());
    }

    #[test]
    fn occurs_indices_already_make_names_unique() {
        let layout = parse(&["01  REC.", "    05  A PIC X OCCURS 3."]);
        let unique: HashSet<&str> = names(&layout).into_iter().collect();
        assert_eq!(unique.len(), 3);
    }

    // --- a realistic whole copybook ----------------------------------------

    #[test]
    fn a_representative_copybook_lays_out_end_to_end() {
        let layout = parse_copybook(
            &book(&[
                "*  CUSTOMER MASTER RECORD",
                "01  CUST-REC.",
                "    05  CUST-ID        PIC 9(8).",
                "    05  CUST-NAME      PIC X(30).",
                "    05  CUST-ADDR.",
                "        10  STREET     PIC X(30).",
                "        10  CITY       PIC X(20).",
                "        10  STATE      PIC X(2).",
                "        10  ZIP        PIC 9(5).",
                "    05  BALANCE        PIC S9(9)V99 COMP-3.",
                "    05  CREDIT-LIMIT   PIC S9(7)V99 COMP-3.",
                "    05  OPEN-DATE      PIC 9(8).",
                "    05  STATUS         PIC X.",
                "        88  ACTIVE     VALUE 'A'.",
                "        88  CLOSED     VALUE 'C'.",
                "    05  ORDER-COUNT    PIC S9(4) COMP.",
                "    05  LAST-ORDERS    OCCURS 3.",
                "        10  ORD-NUM    PIC 9(6).",
                "        10  ORD-AMT    PIC S9(5)V99 COMP-3.",
                "    05  FILLER         PIC X(10).",
            ]),
            false,
            false,
        )
        .unwrap();

        assert_eq!(
            spans(&layout),
            vec![
                ("CUST-ID", 0, 8),
                ("CUST-NAME", 8, 30),
                ("STREET", 38, 30),
                ("CITY", 68, 20),
                ("STATE", 88, 2),
                ("ZIP", 90, 5),
                ("BALANCE", 95, 6),
                ("CREDIT-LIMIT", 101, 5),
                ("OPEN-DATE", 106, 8),
                ("STATUS", 114, 1),
                ("ORDER-COUNT", 115, 2),
                ("ORD-NUM_1", 117, 6),
                ("ORD-AMT_1", 123, 4),
                ("ORD-NUM_2", 127, 6),
                ("ORD-AMT_2", 133, 4),
                ("ORD-NUM_3", 137, 6),
                ("ORD-AMT_3", 143, 4),
            ]
        );
        // The trailing FILLER is dropped as a column but still spans the record.
        assert_eq!(layout.lrecl, 157);
        assert_eq!(kind_of(&layout, "BALANCE"), Kind::Packed { scale: 2 });
        assert_eq!(
            kind_of(&layout, "ORDER-COUNT"),
            Kind::Binary {
                scale: 0,
                signed: true
            }
        );
        assert_eq!(kind_of(&layout, "CUST-NAME"), Kind::Text);
    }
}
