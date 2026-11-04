//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn dialect() -> Dialect {
    Dialect {
        separator: b',',
        quote: Some(b'"'),
        terminator: b'\n',
        comment: None,
    }
}

fn commented(prefix: &str) -> Dialect {
    Dialect {
        comment: Some(prefix.as_bytes().to_vec()),
        ..dialect()
    }
}

fn pieces(content: &[u8], dialect: &Dialect, size: usize) -> Vec<(Vec<u8>, usize)> {
    let mut found = Vec::new();
    let mut rest: Vec<u8> = Vec::new();
    let mut offset = 0;
    loop {
        let data = &content[offset..(offset + size).min(content.len())];
        offset += data.len();
        let last = data.is_empty();
        rest.extend_from_slice(data);
        if rest.is_empty() {
            return found;
        }
        let Cut { end, widest } = cut(&rest, dialect, last);
        if last {
            assert_eq!(end, rest.len(), "the last call takes everything");
            found.push((rest.clone(), widest));
            return found;
        }
        if end > 0 {
            found.push((rest[..end].to_vec(), widest));
            rest.drain(..end);
        }
    }
}

fn widest_of(content: &[u8], dialect: &Dialect) -> usize {
    cut(content, dialect, true).widest
}

#[test]
fn a_partial_record_waits_for_the_next_call() {
    let found = cut(b"a,b\nc,d\ne,f", &dialect(), false);
    assert_eq!(found, Cut { end: 8, widest: 2 });
}

#[test]
fn the_last_call_takes_the_final_record_without_its_terminator() {
    let found = cut(b"a,b\nc,d,e", &dialect(), true);
    assert_eq!(found, Cut { end: 9, widest: 3 });
}

#[test]
fn a_terminator_inside_quotes_does_not_end_the_record() {
    let content = b"a,\"x\ny\"\nb,\"z\"\"\n\"\nc,w\n";
    let found = cut(&content[..9], &dialect(), false);
    assert_eq!(found.end, 8);
    assert_eq!(cut(content, &dialect(), false).end, content.len());
}

#[test]
fn a_quote_inside_an_unquoted_field_is_a_literal() {
    let content = b"5\" pipe,10\nnext,1\n";
    assert_eq!(
        cut(content, &dialect(), false),
        Cut {
            end: content.len(),
            widest: 2
        }
    );
}

#[test]
fn a_quoted_separator_does_not_add_a_field() {
    assert_eq!(widest_of(b"\"a,b\",c\n", &dialect()), 2);
}

#[test]
fn the_widest_record_is_reported() {
    assert_eq!(widest_of(b"a,b\nc,d,e,f\ng,h,i\n", &dialect()), 4);
}

#[test]
fn a_comment_line_is_skipped_quotes_and_all() {
    let content = b"a,b\n# it's \"odd\nc,d\n";
    assert_eq!(
        cut(content, &commented("#"), false),
        Cut {
            end: content.len(),
            widest: 2
        }
    );
}

#[test]
fn a_comment_line_never_counts_as_the_widest() {
    assert_eq!(widest_of(b"# x,y,z,w\na,b\n", &commented("#")), 2);
}

#[test]
fn a_comment_prefix_can_be_several_bytes() {
    let content = b"a,b\n//c,d,e\n/x,y\n";
    assert_eq!(widest_of(content, &commented("//")), 2);
}

#[test]
fn a_prefix_cut_short_by_the_buffer_waits() {
    assert_eq!(cut(b"a,b\n/", &commented("//"), false).end, 4);
    assert_eq!(cut(b"a,b\n/", &commented("//"), true).end, 5);
}

#[test]
fn an_unterminated_comment_waits_unless_last() {
    assert_eq!(cut(b"a,b\n# tail", &commented("#"), false).end, 4);
    assert_eq!(cut(b"a,b\n# tail", &commented("#"), true).end, 10);
}

#[test]
fn a_custom_terminator_ends_records() {
    let dialect = Dialect {
        terminator: b'|',
        ..dialect()
    };
    assert_eq!(
        cut(b"a,b|c,d|e", &dialect, false),
        Cut { end: 8, widest: 2 }
    );
}

#[test]
fn without_quoting_a_quote_is_a_plain_byte() {
    let dialect = Dialect {
        quote: None,
        ..dialect()
    };
    assert_eq!(
        cut(b"\"a,b\nc\n", &dialect, false),
        Cut { end: 7, widest: 2 }
    );
}

#[test]
fn blank_lines_are_skipped() {
    assert_eq!(widest_of(b"\n\na,b\n\n", &dialect()), 2);
}

#[test]
fn nothing_to_cut_in_no_data() {
    assert_eq!(cut(b"", &dialect(), false), Cut::default());
    assert_eq!(cut(b"", &dialect(), true), Cut::default());
}

#[test]
fn a_field_wider_than_the_output_buffer_is_counted_once() {
    let wide = "x".repeat(OUTPUT_SIZE * 3);
    let content = format!("{wide},y\nz\n");
    assert_eq!(widest_of(content.as_bytes(), &dialect()), 2);
}

#[test]
fn a_record_with_more_fields_than_the_ends_buffer_is_counted_whole() {
    let fields = ENDS_SIZE * 3 + 7;
    let content = format!("{}\n", vec!["v"; fields].join(","));
    assert_eq!(widest_of(content.as_bytes(), &dialect()), fields);
}

#[test]
fn any_read_size_cuts_on_record_ends_and_loses_nothing() {
    let content = b"id,text\n1,\"a\nb\"\n# c \"\n2,\"say \"\"hi\"\"\",x\n3,5\" pipe\n4,last";
    let dialect = commented("#");
    for size in [1, 2, 3, 5, 8, 13, 64] {
        let found = pieces(content, &dialect, size);
        let joined: Vec<u8> = found.iter().flat_map(|(piece, _)| piece.clone()).collect();
        assert_eq!(joined, content, "size {size}");
        let widest = found.iter().map(|(_, widest)| *widest).max();
        assert_eq!(widest, Some(3), "size {size}");
        for (piece, _) in &found[..found.len() - 1] {
            assert_eq!(cut(piece, &dialect, false).end, piece.len(), "size {size}");
        }
    }
}
