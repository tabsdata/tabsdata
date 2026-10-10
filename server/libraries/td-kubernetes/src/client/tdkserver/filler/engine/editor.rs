//
// Copyright 2026 Tabsdata Inc.
//

use std::io;
use std::path::Path;

use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use ratatui_textarea::{CursorMove, DataCursor, Input, Key, TextArea};

use super::clipboard::{Clipboard, line_breaks};
use super::highlight::{empty_values, first_empty_value, highlight, locate};
use super::palette::palette;
use crate::client::tdkserver::filler::Heading;

const TAB_LENGTH: u8 = 2;

const SCROLL_LINES: usize = 3;

const KEY_HINTS: [(&str, &str); 8] = [
    ("Ctrl+Q", "Save and quit"),
    ("Ctrl+S", "Save"),
    ("Esc", "Quit without saving"),
    ("Ctrl+Z", "Undo"),
    ("Ctrl+Y", "Redo"),
    ("Shift+Arrows/Drag", "Select"),
    ("Ctrl+C/X/V", "Copy/Cut/Paste"),
    ("Ctrl+A", "Select all"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Saved,
    Discarded,
}

enum Status {
    Hint,
    Saved(usize),
    Error(String),
    ConfirmDiscard,
}

type Selection = Option<((usize, usize), (usize, usize))>;

#[derive(Debug, Clone, Copy, Default)]
pub struct View {
    pub top: usize,
    pub text: Rect,
    pub gutter: usize,
}

impl View {
    pub fn position(&self, lines: &[String], column: u16, row: u16) -> (usize, usize) {
        let last = lines.len().saturating_sub(1);
        let row = (self.top + usize::from(row.saturating_sub(self.text.y))).min(last);
        let column = usize::from(column.saturating_sub(self.text.x))
            .saturating_sub(self.gutter)
            .min(lines.get(row).map_or(0, |line| line.chars().count()));
        (row, column)
    }

    fn contains(&self, column: u16, row: u16) -> bool {
        self.text.contains((column, row).into())
    }
}

struct Screen {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl Screen {
    fn open() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(
            stdout,
            EnterAlternateScreen,
            EnableBracketedPaste,
            EnableMouseCapture
        ) {
            let _ = disable_raw_mode();
            return Err(error);
        }
        match Terminal::new(CrosstermBackend::new(stdout)) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(error) => {
                let _ = restore();
                Err(error)
            }
        }
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        let _ = restore();
    }
}

fn restore() -> io::Result<()> {
    disable_raw_mode()?;
    execute!(
        io::stdout(),
        LeaveAlternateScreen,
        DisableBracketedPaste,
        DisableMouseCapture
    )
}

pub fn edit(
    file: &Path,
    heading: &Heading,
    mut validate: impl FnMut(&Path) -> Result<(), String>,
) -> io::Result<Outcome> {
    let content = std::fs::read_to_string(file)?;
    let mut editor = TextArea::new(content.lines().map(str::to_owned).collect());
    editor.set_tab_length(TAB_LENGTH);
    editor.set_hard_tab_indent(false);
    if let Some(row) = first_empty_value(editor.lines()) {
        jump_to(&mut editor, row);
        editor.move_cursor(CursorMove::Back);
    }

    let mut screen = Screen::open()?;
    let mut view = View::default();
    let mut status = Status::Hint;
    let mut modified = false;
    let mut clipboard = Clipboard::open();

    loop {
        screen
            .terminal
            .draw(|frame| draw(frame, &editor, file, heading, &status, modified, &mut view))?;

        let event = event::read()?;
        if matches!(status, Status::ConfirmDiscard)
            && let Event::Key(KeyEvent {
                code,
                kind: KeyEventKind::Press,
                ..
            }) = event
        {
            if code == KeyCode::Esc {
                return Ok(Outcome::Discarded);
            }
            status = Status::Hint;
            continue;
        }

        match event {
            Event::Mouse(mouse) => mouse_event(&mut editor, &view, mouse),
            Event::Paste(text) => {
                modified |= editor.insert_str(line_breaks(&text));
                status = Status::Hint;
            }
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
                let shift = key.modifiers.contains(KeyModifiers::SHIFT);
                let mut next = Status::Hint;
                match key.code {
                    KeyCode::Char('q') if ctrl => {
                        save(file, editor.lines())?;
                        modified = false;
                        match validate(file) {
                            Ok(()) => return Ok(Outcome::Saved),
                            Err(message) => {
                                if let Some(row) = locate(&message, editor.lines()) {
                                    jump_to(&mut editor, row);
                                }
                                next = Status::Error(message);
                            }
                        }
                    }
                    KeyCode::Char('s') if ctrl => {
                        save(file, editor.lines())?;
                        modified = false;
                        next = Status::Saved(empty_values(editor.lines()));
                    }
                    KeyCode::Esc => {
                        if !modified {
                            return Ok(Outcome::Discarded);
                        }
                        next = Status::ConfirmDiscard;
                    }
                    KeyCode::Char('z') if ctrl => modified |= editor.undo(),
                    KeyCode::Char('y') if ctrl => modified |= editor.redo(),
                    KeyCode::Char('a') if ctrl => editor.select_all(),
                    KeyCode::Char('c') if ctrl => clipboard.copy(&mut editor),
                    KeyCode::Char('x') if ctrl => modified |= clipboard.cut(&mut editor),
                    KeyCode::Char('v') if ctrl => modified |= clipboard.paste(&mut editor),
                    KeyCode::Left
                    | KeyCode::Right
                    | KeyCode::Up
                    | KeyCode::Down
                    | KeyCode::Home
                    | KeyCode::End
                    | KeyCode::PageUp
                    | KeyCode::PageDown => {
                        if shift && !editor.is_selecting() {
                            editor.start_selection();
                        } else if !shift {
                            editor.cancel_selection();
                        }
                        editor.move_cursor(movement(key.code, ctrl));
                    }
                    _ => {
                        let input: Input = Event::Key(key).into();
                        if input.key != Key::Null {
                            modified |= editor.input_without_shortcuts(input);
                        }
                    }
                }
                status = next;
            }
            _ => {}
        }
    }
}

fn mouse_event(editor: &mut TextArea, view: &View, mouse: MouseEvent) {
    let MouseEvent {
        kind, column, row, ..
    } = mouse;
    match kind {
        MouseEventKind::Down(MouseButton::Left) if view.contains(column, row) => {
            editor.cancel_selection();
            move_to(editor, view.position(editor.lines(), column, row));
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if !editor.is_selecting() {
                editor.start_selection();
            }
            move_to(editor, view.position(editor.lines(), column, row));
        }
        MouseEventKind::ScrollUp => {
            (0..SCROLL_LINES).for_each(|_| editor.move_cursor(CursorMove::Up))
        }
        MouseEventKind::ScrollDown => {
            (0..SCROLL_LINES).for_each(|_| editor.move_cursor(CursorMove::Down))
        }
        _ => {}
    }
}

fn move_to(editor: &mut TextArea, (row, column): (usize, usize)) {
    editor.move_cursor(CursorMove::Jump(
        u16::try_from(row).unwrap_or(u16::MAX),
        u16::try_from(column).unwrap_or(u16::MAX),
    ));
}

fn jump_to(editor: &mut TextArea, row: usize) {
    editor.move_cursor(CursorMove::Jump(u16::try_from(row).unwrap_or(u16::MAX), 0));
    editor.move_cursor(CursorMove::End);
}

fn save(file: &Path, lines: &[String]) -> io::Result<()> {
    std::fs::write(file, lines.join("\n") + "\n")
}

fn movement(code: KeyCode, ctrl: bool) -> CursorMove {
    match (code, ctrl) {
        (KeyCode::Left, true) => CursorMove::WordBack,
        (KeyCode::Right, true) => CursorMove::WordForward,
        (KeyCode::Left, false) => CursorMove::Back,
        (KeyCode::Right, false) => CursorMove::Forward,
        (KeyCode::Up, _) => CursorMove::Up,
        (KeyCode::Down, _) => CursorMove::Down,
        (KeyCode::Home, true) | (KeyCode::PageUp, _) => CursorMove::Top,
        (KeyCode::End, true) | (KeyCode::PageDown, _) => CursorMove::Bottom,
        (KeyCode::Home, false) => CursorMove::Head,
        _ => CursorMove::End,
    }
}

fn draw(
    frame: &mut Frame,
    editor: &TextArea,
    file: &Path,
    heading: &Heading,
    status: &Status,
    modified: bool,
    view: &mut View,
) {
    let colors = palette();
    frame.render_widget(
        Block::default().style(Style::default().bg(colors.base).fg(colors.text)),
        frame.area(),
    );
    let [title, body, state, keys] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    frame.render_widget(
        Paragraph::new(title_line(heading, modified)).style(Style::default().bg(colors.mantle)),
        title,
    );
    draw_body(frame, editor, file, body, view);
    frame.render_widget(Paragraph::new(status_line(status)), state);
    frame.render_widget(
        Paragraph::new(key_hints()).style(Style::default().bg(colors.mantle)),
        keys,
    );
}

fn title_line(heading: &Heading, modified: bool) -> Line<'static> {
    let colors = palette();
    let label = Style::default().fg(colors.subtext);
    let value = Style::default()
        .fg(colors.text)
        .add_modifier(Modifier::BOLD);
    let mut spans = vec![
        Span::styled(
            " Fill the settings ",
            Style::default()
                .bg(colors.key)
                .fg(colors.base)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Instance ", label),
        Span::styled(heading.instance.clone(), value),
        Span::styled("  Provider ", label),
        Span::styled(heading.provider.clone(), value),
    ];
    if modified {
        spans.push(Span::styled(
            "  Modified",
            Style::default()
                .fg(colors.required)
                .add_modifier(Modifier::BOLD),
        ));
    }
    Line::from(spans)
}

fn draw_body(frame: &mut Frame, editor: &TextArea, file: &Path, area: Rect, view: &mut View) {
    let colors = palette();
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(colors.overlay))
        .title(Span::styled(
            format!(" {} ", file.display()),
            Style::default().fg(colors.key).add_modifier(Modifier::BOLD),
        ));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let DataCursor(row, column) = editor.cursor();
    let height = usize::from(inner.height);
    let top = &mut view.top;
    if row < *top {
        *top = row;
    } else if height > 0 && row >= *top + height {
        *top = row + 1 - height;
    }
    let lines = editor.lines();
    let gutter = lines.len().to_string().len() + 2;
    view.text = inner;
    view.gutter = gutter;
    let selection = editor.selection_range();
    let visible: Vec<Line> = lines
        .iter()
        .enumerate()
        .skip(view.top)
        .take(height)
        .map(|(index, text)| render_line(index, text, gutter, inner.width, index == row, selection))
        .collect();
    frame.render_widget(Paragraph::new(visible), inner);

    let offset = gutter + lines[row].chars().take(column).count();
    let x = inner
        .x
        .saturating_add(u16::try_from(offset).unwrap_or(u16::MAX));
    let y = inner.y + u16::try_from(row - view.top).unwrap_or(0);
    frame.set_cursor_position((x.min(inner.right().saturating_sub(1)), y));
}

fn render_line(
    index: usize,
    text: &str,
    gutter: usize,
    width: u16,
    current: bool,
    selection: Selection,
) -> Line<'static> {
    let colors = palette();
    let background = if current { colors.surface } else { colors.base };
    let number = if current {
        Style::default()
            .fg(colors.text)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(colors.overlay)
    };
    let mut spans = vec![Span::styled(
        format!("{:>width$}  ", index + 1, width = gutter - 2),
        number.bg(background),
    )];
    let styles = highlight(text);
    let mut run = String::new();
    let mut run_style: Option<Style> = None;
    for (position, character) in text.chars().enumerate() {
        let mut style = styles[position];
        if style.bg.is_none() {
            style = style.bg(background);
        }
        if selected(selection, index, position) {
            style = style.bg(colors.selection);
        }
        if run_style != Some(style) {
            if let Some(previous) = run_style {
                spans.push(Span::styled(std::mem::take(&mut run), previous));
            }
            run_style = Some(style);
        }
        run.push(character);
    }
    if let Some(previous) = run_style {
        spans.push(Span::styled(run, previous));
    }
    let used = gutter + text.chars().count();
    spans.push(Span::styled(
        " ".repeat(usize::from(width).saturating_sub(used)),
        Style::default().bg(background),
    ));
    Line::from(spans)
}

fn selected(selection: Selection, row: usize, column: usize) -> bool {
    selection.is_some_and(|(start, end)| (row, column) >= start && (row, column) < end)
}

fn status_line(status: &Status) -> Line<'static> {
    let colors = palette();
    let banner = |text: String, background| {
        Line::from(Span::styled(
            text,
            Style::default()
                .fg(colors.base)
                .bg(background)
                .add_modifier(Modifier::BOLD),
        ))
    };
    match status {
        Status::Hint => {
            let label = Style::default().fg(colors.subtext);
            Line::from(vec![
                Span::styled(" Values marked ", label),
                Span::styled(
                    " \"\" ",
                    Style::default()
                        .bg(colors.required)
                        .fg(colors.base)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" are required. Values starting with ", label),
                Span::styled(
                    "env:",
                    Style::default()
                        .fg(colors.reference)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(" are read from that environment variable.", label),
            ])
        }
        Status::Saved(0) => banner(
            " Saved. Every required value is filled; press Ctrl+Q to continue.".to_owned(),
            colors.string,
        ),
        Status::Saved(missing) => banner(
            format!(" Saved. Required values still empty: {missing}."),
            colors.required,
        ),
        Status::Error(message) => banner(format!(" {message}"), colors.error),
        Status::ConfirmDiscard => banner(
            " The changes are not saved. Press Esc again to quit without saving, or any other \
             key to keep editing."
                .to_owned(),
            colors.required,
        ),
    }
}

fn key_hints() -> Line<'static> {
    let colors = palette();
    let chip = Style::default()
        .fg(colors.text)
        .bg(colors.selection)
        .add_modifier(Modifier::BOLD);
    let label = Style::default().fg(colors.subtext);
    let mut spans = Vec::new();
    for (keys, action) in KEY_HINTS {
        spans.push(Span::raw(" "));
        spans.push(Span::styled(format!(" {keys} "), chip));
        spans.push(Span::styled(format!(" {action} "), label));
    }
    Line::from(spans)
}
