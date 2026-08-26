//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

fn recorded() -> (Option<String>, Option<String>) {
    SCOPE.with(|cell| {
        let scope = cell.borrow();
        (scope.instance.clone(), scope.operation.clone())
    })
}

fn entered() -> bool {
    SCOPE.with(|cell| cell.borrow().held.is_some())
}

#[test]
fn opening_enters_a_scope_with_nothing_recorded() {
    open();
    assert_eq!(recorded(), (None, None));
    assert!(entered());
}

#[test]
fn an_operation_survives_a_later_instance() {
    open();
    set_operation("service");
    set_instance("an-instance");
    assert_eq!(
        recorded(),
        (Some("an-instance".to_owned()), Some("service".to_owned()))
    );
}

#[test]
fn an_instance_survives_a_later_operation() {
    open();
    set_instance("an-instance");
    set_operation("service");
    assert_eq!(
        recorded(),
        (Some("an-instance".to_owned()), Some("service".to_owned()))
    );
}

#[test]
fn a_later_instance_replaces_the_earlier_one() {
    open();
    set_instance("first");
    set_instance("second");
    assert_eq!(recorded().0, Some("second".to_owned()));
    assert!(entered());
}

#[test]
fn only_one_scope_is_ever_held() {
    open();
    set_operation("service");
    set_instance("an-instance");
    set_instance("another-instance");
    assert!(entered());
}

#[test]
fn a_carried_scope_travels_to_another_thread() {
    open();
    set_instance("an-instance");
    let carried = carried();
    std::thread::spawn(move || {
        let _entered = carried.enter();
    })
    .join()
    .expect("the thread completes");
}

fn argv(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

#[test]
fn the_operation_is_the_first_bare_argument() {
    assert_eq!(
        operation_of(argv("tdkserver status --name an-instance")),
        Some("status".to_owned())
    );
}

#[test]
fn leading_flags_do_not_hide_the_operation() {
    assert_eq!(
        operation_of(argv("tdkserver --verbose venv update")),
        Some("venv".to_owned())
    );
}

#[test]
fn a_bare_invocation_has_no_operation() {
    assert_eq!(operation_of(argv("tdkserver")), None);
    assert_eq!(operation_of(argv("tdkserver --help")), None);
}
