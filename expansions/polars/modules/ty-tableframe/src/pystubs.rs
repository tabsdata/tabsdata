//
// Copyright 2025 Tabs Data Inc.
//

use pyo3_stub_gen::Result;
use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
};

const TD_VCORE_PATH: &str = "TD_VCORE_PATH";
const STUBS_SENTINEL: &str = ".ok.pystubs";

const SLASH: &str = "/";
const BACKSLASH: &str = "\\";

fn main() -> Result<()> {
    let sentinel = sentinel();

    let vcore_exists = vcore_available();
    let sentinel_exists = sentinel_available(&sentinel);

    if vcore_exists {
        if sentinel_exists {
            println!(
                "🍎 Variable td_vcore_path exists & sentinel .ok.pystubs exists: skipping python stubs generation"
            );

            Ok(())
        } else {
            println!(
                "🍏 Variable td_vcore_path exists & sentinel .ok.pystubs does not exist: running python stubs generation"
            );

            let stub = ty_tableframe::stub_info()?;
            stub.generate()?;
            remove_init_pyi_stubs();

            if let Some(ref sentinel_path) = sentinel {
                create_home(sentinel_path)?;
                create_sentinel(sentinel_path);
            }

            Ok(())
        }
    } else {
        println!("🍊 Variable td_vcore_path does not exist: running python stubs generation");

        let stub = ty_tableframe::stub_info()?;
        stub.generate()?;
        remove_init_pyi_stubs();

        Ok(())
    }
}

//noinspection DuplicatedCode
fn vcore_available() -> bool {
    match env(TD_VCORE_PATH) {
        Some(path) if !path.is_empty() => {
            println!("🍋 Variable td_vcore_path found: '{path}'");
            true
        }
        None => {
            println!("🍋 Variable td_vcore_path missing");
            false
        }
        Some(_) => {
            println!("🍋 Variable td_vcore_path empty");
            false
        }
    }
}

//noinspection DuplicatedCode
fn sentinel_available(sentinel: &Option<PathBuf>) -> bool {
    let Some(file) = sentinel else {
        println!("❗ No sentinel home folder provided");
        return false;
    };
    if file.exists() {
        println!("❗ The sentinel file already exists: '{}'", file.display());
        true
    } else {
        println!("❕ The sentinel file does no exist: '{}'", file.display());
        false
    }
}

//noinspection DuplicatedCode
fn sentinel() -> Option<PathBuf> {
    env::args_os().nth(1).map(|arg| {
        let path = arg.to_string_lossy();

        #[cfg(windows)]
        let path = path.replace(SLASH, BACKSLASH);

        #[cfg(not(windows))]
        let path = path.replace(BACKSLASH, SLASH);

        PathBuf::from(path).join(STUBS_SENTINEL)
    })
}

//noinspection DuplicatedCode
fn create_home(path: &Path) -> Result<()> {
    if let Some(sentinel) = path.parent() {
        fs::create_dir_all(sentinel).map_err(|error| {
            eprintln!(
                "⁉️ Unable to create folder {}: {}",
                sentinel.display(),
                error
            );
            error
        })?;
    }
    Ok(())
}

//noinspection DuplicatedCode
fn create_sentinel(sentinel: &Path) {
    match File::create(sentinel) {
        Ok(_) => println!(
            "🍪 Created sentinel file {}; future python stubs generation runs will be skipped",
            sentinel.display()
        ),
        Err(err) => eprintln!(
            "⁉️ Unable to create sentinel file {}: {}",
            sentinel.display(),
            err
        ),
    }
}

//noinspection DuplicatedCode
fn env(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => None,
    }
}

//noinspection DuplicatedCode
fn remove_init_pyi_stubs() {
    let cargo_manifest_dir = env!("CARGO_MANIFEST_DIR");
    let python_folder = PathBuf::from(cargo_manifest_dir).join("python");
    println!(
        "🧹 Removing __init__.pyi files from: {}",
        python_folder.display()
    );
    if let Err(err) = remove_init_pyi_files(&python_folder) {
        eprintln!("⚠️ Error during stub removal: {}", err);
    }
}

//noinspection DuplicatedCode
fn remove_init_pyi_files(root: &Path) -> std::io::Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            remove_init_pyi_files(&path)?;
        } else if path.file_name().is_some_and(|n| n == "__init__.pyi") {
            match fs::remove_file(&path) {
                Ok(_) => println!("🧹 ✅ Removed: {}", path.display()),
                Err(err) => eprintln!("⚠️ Failed to remove {}: {}", path.display(), err),
            }
        }
    }

    Ok(())
}
