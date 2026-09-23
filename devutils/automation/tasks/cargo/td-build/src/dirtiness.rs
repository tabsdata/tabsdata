//
// Copyright 2026 Tabsdata Inc.
//

use anyhow::{Context, Result, bail};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;

const SYMLINK_MODE: &str = "120000";
const SUBMODULE_MODE: &str = "160000";
const MAX_LINK_DEPTH: usize = 32;

pub const TD_CHECK_DIRTINESS: &str = "TD_CHECK_DIRTINESS";

pub fn strict_dirtiness(on_ga: bool) -> bool {
    on_ga
        || std::env::var(TD_CHECK_DIRTINESS)
            .is_ok_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub status: String,
    pub path: String,
    pub reason: Option<String>,
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reason {
            Some(reason) => write!(f, "{} {} ({})", self.status, self.path, reason),
            None => write!(f, "{} {}", self.status, self.path),
        }
    }
}

struct IndexEntry {
    mode: String,
    object: String,
    path: String,
}

fn git(folder: &Path, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(folder)
        .args(arguments)
        .output()
        .with_context(|| {
            format!(
                "Failed to run 'git {}' in '{}'",
                arguments.join(" "),
                folder.display()
            )
        })?;
    if !output.status.success() {
        bail!(
            "'git {}' failed in '{}': {}",
            arguments.join(" "),
            folder.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

fn records(output: &[u8]) -> impl Iterator<Item = String> + '_ {
    output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .map(|record| String::from_utf8_lossy(record).into_owned())
}

fn tracked_changes(repository: &Path) -> Result<Vec<Change>> {
    let output = git(
        repository,
        &["status", "--porcelain=v1", "-z", "--untracked-files=no"],
    )?;
    let mut changes = Vec::new();
    let mut records = records(&output);
    while let Some(record) = records.next() {
        if record.len() < 4 {
            continue;
        }
        let status = record[..2].to_string();
        if status.contains('R') || status.contains('C') {
            records.next();
        }
        changes.push(Change {
            status,
            path: record[3..].to_string(),
            reason: None,
        });
    }
    Ok(changes)
}

fn index_entries(folder: &Path, pathspec: &str) -> Result<Vec<IndexEntry>> {
    let output = git(folder, &["ls-files", "--stage", "-z", "--", pathspec])?;
    Ok(records(&output)
        .filter_map(|record| {
            let (header, path) = record.split_once('\t')?;
            let mut fields = header.split_whitespace();
            Some(IndexEntry {
                mode: fields.next()?.to_string(),
                object: fields.next()?.to_string(),
                path: path.to_string(),
            })
        })
        .collect())
}

fn same_file(materialized: &Path, source: &Path) -> Result<Option<String>> {
    let source_content = match fs::read(source) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok((materialized.symlink_metadata().is_ok())
                .then(|| format!("'{}' does not exist", source.display())));
        }
        Err(error) => {
            return Err(error).with_context(|| format!("Failed to read '{}'", source.display()));
        }
    };
    match fs::read(materialized) {
        Ok(content) if content == source_content => Ok(None),
        Ok(_) => Ok(Some(format!(
            "'{}' differs from '{}'",
            materialized.display(),
            source.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            Ok(Some(format!("'{}' is missing", materialized.display())))
        }
        Err(error) => {
            Err(error).with_context(|| format!("Failed to read '{}'", materialized.display()))
        }
    }
}

fn canonical(path: &Path) -> std::io::Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    #[cfg(windows)]
    if let Some(stripped) = canonical
        .to_str()
        .and_then(|path| path.strip_prefix(r"\\?\"))
        .filter(|path| !path.starts_with(r"UNC\"))
    {
        return Ok(PathBuf::from(stripped));
    }
    Ok(canonical)
}

fn same_link(materialized: &Path, source: &Path) -> bool {
    materialized
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.file_type().is_symlink())
        && matches!(
            (fs::read_link(materialized), fs::read_link(source)),
            (Ok(materialized_target), Ok(source_target)) if materialized_target == source_target
        )
}

fn compare_materialization(
    materialized: &Path,
    source: &Path,
    depth: usize,
) -> Result<Option<String>> {
    if depth > MAX_LINK_DEPTH {
        return Ok(Some(format!(
            "symbolic link chain at '{}' is too deep",
            source.display()
        )));
    }
    let source = match canonical(source) {
        Ok(source) => source,
        Err(_) => {
            return Ok(Some(format!(
                "symbolic link target '{}' does not exist",
                source.display()
            )));
        }
    };
    if source.is_file() {
        return same_file(materialized, &source);
    }
    if !source.is_dir() {
        return Ok(Some(format!(
            "symbolic link target '{}' is neither a file nor a folder",
            source.display()
        )));
    }
    if !materialized.is_dir() {
        return Ok(Some(format!(
            "'{}' is not a folder, but its symbolic link target '{}' is",
            materialized.display(),
            source.display()
        )));
    }
    for entry in index_entries(&source, ".")? {
        if entry.mode == SUBMODULE_MODE {
            continue;
        }
        let materialized_entry = materialized.join(&entry.path);
        let source_entry = source.join(&entry.path);
        let difference = if entry.mode == SYMLINK_MODE {
            if same_link(&materialized_entry, &source_entry) {
                None
            } else {
                compare_materialization(&materialized_entry, &source_entry, depth + 1)?
            }
        } else {
            same_file(&materialized_entry, &source_entry)?
        };
        if difference.is_some() {
            return Ok(difference);
        }
    }
    Ok(None)
}

enum Verdict {
    Materialized,
    Changed(Option<String>),
}

fn materialization_verdict(repository: &Path, change: &Change) -> Result<Verdict> {
    let mut status = change.status.chars();
    let (staged, unstaged) = (status.next(), status.next());
    if staged != Some(' ') || !matches!(unstaged, Some('T') | Some('D')) {
        return Ok(Verdict::Changed(None));
    }
    let entry = match index_entries(repository, &change.path)?
        .into_iter()
        .find(|entry| entry.path == change.path)
    {
        Some(entry) if entry.mode == SYMLINK_MODE => entry,
        _ => return Ok(Verdict::Changed(None)),
    };
    let materialized = repository.join(&change.path);
    match materialized.symlink_metadata() {
        Err(_) => {
            return Ok(Verdict::Changed(Some(
                "symbolic link was deleted".to_string(),
            )));
        }
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Ok(Verdict::Changed(Some(
                "symbolic link was retargeted".to_string(),
            )));
        }
        Ok(_) => {}
    }
    let target = git(repository, &["cat-file", "blob", &entry.object])?;
    let target = PathBuf::from(String::from_utf8_lossy(&target).trim_end_matches('\n'));
    let source = materialized
        .parent()
        .map(|parent| parent.join(&target))
        .unwrap_or(target);
    Ok(match compare_materialization(&materialized, &source, 0)? {
        None => Verdict::Materialized,
        Some(difference) => Verdict::Changed(Some(format!(
            "materialized symbolic link differs: {}",
            difference
        ))),
    })
}

pub fn changed_tracked_files(repository: &Path, strict: bool) -> Result<Vec<Change>> {
    let changes = tracked_changes(repository)?;
    if !strict {
        return Ok(changes);
    }
    let mut real_changes = Vec::new();
    for change in changes {
        if let Verdict::Changed(reason) = materialization_verdict(repository, &change)? {
            real_changes.push(Change { reason, ..change });
        }
    }
    Ok(real_changes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn scratch() -> Scratch {
        let path = std::env::temp_dir().join(format!(
            "td-build-dirtiness-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Scratch(fs::canonicalize(&path).unwrap())
    }

    fn run(folder: &Path, arguments: &[&str]) {
        git(folder, arguments).unwrap();
    }

    fn init(path: &Path) {
        fs::create_dir_all(path).unwrap();
        run(path, &["init", "-q"]);
        run(path, &["config", "user.email", "test@tabsdata.com"]);
        run(path, &["config", "user.name", "test"]);
        run(path, &["config", "core.symlinks", "true"]);
    }

    fn commit(path: &Path) {
        run(path, &["add", "-A"]);
        run(path, &["commit", "-q", "-m", "init"]);
    }

    fn copy_folder(source: &Path, destination: &Path) {
        fs::create_dir_all(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = destination.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_folder(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    fn materialize(repository: &Path, link: &str) {
        let path = repository.join(link);
        let target = fs::canonicalize(&path).unwrap();
        fs::remove_file(&path).unwrap();
        if target.is_dir() {
            copy_folder(&target, &path);
        } else {
            fs::copy(&target, &path).unwrap();
        }
    }

    #[cfg(unix)]
    fn layout() -> (Scratch, PathBuf, PathBuf) {
        let scratch = scratch();
        let owner = scratch.0.join("owner");
        let overlay = scratch.0.join("overlay");
        init(&owner);
        fs::create_dir_all(owner.join("folder/nested")).unwrap();
        fs::write(owner.join("file.txt"), "file").unwrap();
        fs::write(owner.join("folder/a.txt"), "a").unwrap();
        fs::write(owner.join("folder/nested/b.txt"), "b").unwrap();
        commit(&owner);
        init(&overlay);
        std::os::unix::fs::symlink("../owner/file.txt", overlay.join("file.txt")).unwrap();
        std::os::unix::fs::symlink("../owner/folder", overlay.join("folder")).unwrap();
        fs::write(overlay.join("own.txt"), "own").unwrap();
        commit(&overlay);
        (scratch, owner, overlay)
    }

    #[cfg(unix)]
    #[test]
    fn identical_materializations_are_clean_in_strict_mode_only() {
        let (_scratch, _owner, overlay) = layout();
        materialize(&overlay, "file.txt");
        materialize(&overlay, "folder");
        fs::write(overlay.join("folder/untracked.log"), "build output").unwrap();
        assert!(changed_tracked_files(&overlay, true).unwrap().is_empty());
        assert_eq!(changed_tracked_files(&overlay, false).unwrap().len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn modified_materializations_are_real_changes() {
        let (_scratch, _owner, overlay) = layout();
        materialize(&overlay, "file.txt");
        materialize(&overlay, "folder");
        fs::write(overlay.join("file.txt"), "changed").unwrap();
        fs::remove_file(overlay.join("folder/nested/b.txt")).unwrap();
        let changes = changed_tracked_files(&overlay, true).unwrap();
        assert_eq!(changes.len(), 2);
        assert!(changes.iter().all(|change| change.reason.is_some()));
        assert!(
            changed_tracked_files(&overlay, true)
                .unwrap()
                .iter()
                .all(|change| change.status.starts_with(' '))
        );
    }

    #[cfg(unix)]
    #[test]
    fn regular_changes_are_real_changes_in_strict_mode() {
        let (_scratch, _owner, overlay) = layout();
        fs::write(overlay.join("own.txt"), "changed").unwrap();
        fs::remove_file(overlay.join("file.txt")).unwrap();
        let changes = changed_tracked_files(&overlay, true).unwrap();
        let paths: Vec<_> = changes.iter().map(|change| change.path.as_str()).collect();
        assert_eq!(paths, vec!["file.txt", "own.txt"]);
        assert!(changes[0].reason.is_some());
        assert!(changes[1].reason.is_none());
    }
}
