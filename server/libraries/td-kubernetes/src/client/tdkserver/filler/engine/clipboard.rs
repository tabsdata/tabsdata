//
// Copyright 2026 Tabsdata Inc.
//

use ratatui_textarea::TextArea;

pub struct Clipboard {
    system: Option<arboard::Clipboard>,
}

impl Clipboard {
    pub fn open() -> Self {
        Self {
            system: arboard::Clipboard::new().ok(),
        }
    }

    pub fn copy(&mut self, editor: &mut TextArea) {
        editor.copy();
        self.publish(editor);
    }

    pub fn cut(&mut self, editor: &mut TextArea) -> bool {
        let modified = editor.cut();
        if modified {
            self.publish(editor);
        }
        modified
    }

    pub fn paste(&mut self, editor: &mut TextArea) -> bool {
        match self
            .system
            .as_mut()
            .and_then(|system| system.get_text().ok())
        {
            Some(text) => editor.insert_str(line_breaks(&text)),
            None => editor.paste(),
        }
    }

    fn publish(&mut self, editor: &TextArea) {
        if let Some(system) = self.system.as_mut() {
            let _ = system.set_text(editor.yank_text());
        }
    }
}

pub fn line_breaks(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
