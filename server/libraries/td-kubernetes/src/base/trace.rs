//
// Copyright 2026 Tabsdata Inc.
//

use std::cell::RefCell;

use tracing::{Span, info_span, span::EnteredSpan};

use crate::base::constants::UNDEFINED_FIELD;

#[derive(Default)]
struct Scope {
    instance: Option<String>,
    operation: Option<String>,
    held: Option<EnteredSpan>,
}

thread_local! {
    static SCOPE: RefCell<Scope> = RefCell::new(Scope::default());
}

pub fn open() {
    reopen(|_| {});
}

pub fn set_operation(operation: &str) {
    let operation = operation.to_owned();
    reopen(move |scope| scope.operation = Some(operation));
}

pub fn set_instance(instance: &str) {
    let instance = instance.to_owned();
    reopen(move |scope| scope.instance = Some(instance));
}

pub fn carried() -> Span {
    Span::current()
}

pub fn operation_of(arguments: impl IntoIterator<Item = String>) -> Option<String> {
    arguments
        .into_iter()
        .skip(1)
        .find(|argument| !argument.starts_with('-'))
}

fn reopen(update: impl FnOnce(&mut Scope)) {
    let (instance, operation) = SCOPE.with(|cell| {
        let mut scope = cell.borrow_mut();
        update(&mut scope);
        scope.held = None;
        (scope.instance.clone(), scope.operation.clone())
    });
    let span = info_span!(
        "tdkserver",
        instance = instance.as_deref().unwrap_or(UNDEFINED_FIELD),
        operation = operation.as_deref().unwrap_or(UNDEFINED_FIELD)
    );
    SCOPE.with(|cell| cell.borrow_mut().held = Some(span.entered()));
}

#[cfg(test)]
mod tests;
