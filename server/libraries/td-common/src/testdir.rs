//
// Copyright 2024 Tabs Data Inc.
//

use std::cell::RefCell;
use std::path::PathBuf;

/// Creates a test-specific directory under `target/testdir/`, cleaning any
/// previous contents from prior runs.
///
/// This replaces `testdir!()` which uses numbered directories with a global
/// cleanup policy (keeping the 8 most recent). Under nextest, where each test
/// runs in its own process, multiple processes race to create numbered dirs
/// and the cleanup can delete directories still in use by long-running tests,
/// causing spurious "No such file or directory" failures.
///
/// This macro avoids the issue by scoping cleanup to the individual test's
/// own directory — no shared numbered dirs, no cross-test interference.
///
/// The directory is cleaned only on the first call within a test thread.
/// Subsequent calls return the same path without wiping it, so tests and
/// their helpers can safely call `tdtestdir!()` multiple times. This works
/// correctly both under nextest (one test per process) and regular `cargo
/// test` (one thread per test).
///
/// Uses `module_path!()` to include the calling crate's name in the directory
/// path (the thread name alone omits it), ensuring unique directories even
/// across crates with identical module structures. The test function name
/// comes from the thread name, which the test harness sets to the full
/// test path within the crate.
#[macro_export]
macro_rules! tdtestdir {
    () => {
        $crate::testdir::_tdtestdir(module_path!())
    };
}

/// The workspace root, located by walking up from the running executable for a
/// `.root` marker, else a `Cargo.toml` + `target/` dir (see [`find_root`]). Shared
/// test infrastructure uses this to anchor stable, per-workspace paths (e.g. the
/// shared test postgres data dir under `target/`).
pub fn root() -> PathBuf {
    find_root()
}

fn find_root() -> PathBuf {
    let start = std::env::current_exe().unwrap();

    let mut path = start.clone();
    loop {
        if path.join(".root").is_file() {
            return path;
        }
        if !path.pop() {
            break;
        }
    }

    let mut path = start.clone();
    loop {
        if path.join("Cargo.toml").is_file() && path.join("target").is_dir() {
            return path;
        }
        if !path.pop() {
            break;
        }
    }

    let mut path = start.clone();
    loop {
        if path.join("Cargo.toml").is_file() {
            return path;
        }
        if !path.pop() {
            break;
        }
    }

    let mut path = start;
    loop {
        if path.join("target").is_dir() {
            return path;
        }
        if !path.pop() {
            panic!("Could not find .root, Cargo.toml, or target in any parent directory");
        }
    }
}

pub fn _tdtestdir(module_path: &str) -> PathBuf {
    thread_local! {
        static DIR: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }
    DIR.with(|cell| {
        let existing = cell.borrow();
        if let Some(dir) = existing.as_ref() {
            return dir.clone();
        }
        drop(existing);

        let root = find_root();

        // module_path!() gives the crate + module, e.g. "td_shuttle::transporter::cli::tests"
        // thread name gives the test path within the crate, e.g. "transporter::cli::tests::test_copy"
        // We take the crate name from module_path (first segment) and append the thread name
        // to get a unique per-test directory: target/testdir/td_shuttle/transporter/cli/tests/test_copy
        let crate_name = module_path.split("::").next().unwrap_or("unknown");
        let test_name = std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_string();
        let dir = test_name.split("::").fold(
            root.join("target").join("testdir").join(crate_name),
            |dir, segment| dir.join(segment),
        );
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        *cell.borrow_mut() = Some(dir.clone());
        dir
    })
}
