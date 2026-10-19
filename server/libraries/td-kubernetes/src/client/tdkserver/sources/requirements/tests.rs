//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::PathBuf;

use indoc::indoc;

use super::*;
use crate::base::constants::Venv;

const BASE: &str = indoc! {"
    #
    # Copyright 2026 Tabsdata Inc.
    #

    # Complementary packages for the virtual environment.
"};

const EXTRA: &str = indoc! {"
    # drivers
    --index-url https://example.com/simple
    psycopg2-binary==2.9.12  # postgres
    oracledb==3.2.0
"};

fn source(folder: &Path, contents: &str) -> PathBuf {
    let file = folder.join("extra.txt");
    fs::write(&file, contents).expect("the source is written");
    file
}

fn init_folder(folder: &Path, base: &str) -> PathBuf {
    let init = folder.join("init");
    let assets = init.join(KPROFILE_ASSETS_FAMILY.folder);
    fs::create_dir_all(&assets).expect("the assets folder is created");
    for venv in VENVS {
        fs::write(assets.join(venv.requirements_asset()), base).expect("the asset is written");
    }
    init
}

fn asset(init: &Path, venv: &Venv) -> String {
    fs::read_to_string(
        init.join(KPROFILE_ASSETS_FAMILY.folder)
            .join(venv.requirements_asset()),
    )
    .expect("the asset is read")
}

#[test]
fn every_base_venv_gets_the_requirements_after_its_own() {
    let temp = tempfile::tempdir().expect("tempdir");
    let file = source(temp.path(), EXTRA);
    let init = init_folder(temp.path(), BASE);

    ExtraRequirements::load(&file)
        .expect("the requirements load")
        .merge(&init)
        .expect("the requirements merge");

    let expected = format!("{BASE}\n# From --requirements {}\n{EXTRA}", file.display());
    for venv in VENVS {
        assert_eq!(asset(&init, venv), expected, "{}", venv.as_str());
    }
}

#[test]
fn a_file_without_a_final_newline_still_ends_in_one() {
    let temp = tempfile::tempdir().expect("tempdir");
    let file = source(temp.path(), "polars==1.0.0");
    let init = init_folder(temp.path(), "# base");

    ExtraRequirements::load(&file)
        .expect("the requirements load")
        .merge(&init)
        .expect("the requirements merge");

    let expected = format!(
        "# base\n\n# From --requirements {}\npolars==1.0.0\n",
        file.display()
    );
    for venv in VENVS {
        assert_eq!(asset(&init, venv), expected, "{}", venv.as_str());
    }
}

#[test]
fn an_invalid_requirement_is_refused_with_its_line() {
    let temp = tempfile::tempdir().expect("tempdir");
    let file = source(temp.path(), "polars==1.0.0\nnot a requirement ===\n");

    let error = ExtraRequirements::load(&file).expect_err("the file is refused");
    let message = error
        .source()
        .expect("the rejection carries the cause")
        .to_string();
    assert!(message.contains("line 2"), "{message}");
}

#[test]
fn a_missing_file_is_refused() {
    let temp = tempfile::tempdir().expect("tempdir");
    assert!(ExtraRequirements::load(&temp.path().join("absent.txt")).is_err());
}
