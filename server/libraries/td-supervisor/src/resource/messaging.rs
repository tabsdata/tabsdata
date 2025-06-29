//
// Copyright 2024 Tabs Data Inc.
//

use std::fs;
use std::path::PathBuf;
use td_common::files::{
    get_files_in_folder_sorted_by_name, get_files_in_subfolders_sorted_by_name, YAML_EXTENSION,
};
use td_common::server::{
    PayloadType, SupervisorMessage, COMPLETE_FOLDER, ERROR_FOLDER, FAIL_FOLDER, ONGOING_FOLDER,
    PLANNED_FOLDER, QUEUED_FOLDER,
};
use tracing::{debug, error};

const FOLDERS: [&str; 3] = [PLANNED_FOLDER, QUEUED_FOLDER, ONGOING_FOLDER];

#[derive(Debug)]
pub struct SupervisorMessageQueue {
    root: PathBuf,
}

impl SupervisorMessageQueue {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn planned_messages(&self) -> Vec<SupervisorMessage> {
        get_files_in_folder_sorted_by_name(self.root.join(PLANNED_FOLDER), Some(YAML_EXTENSION))
            .unwrap_or_else(|_| Vec::new())
            .into_iter()
            .filter_map(|file| {
                match SupervisorMessage::try_from((file.clone(), PayloadType::Request)) {
                    Ok(msg) => Some(msg),
                    Err(e) => {
                        error!("Failed to extract message from file {:?}: {:?}", file, e);
                        None
                    }
                }
            })
            .collect()
    }

    pub fn error_messages(&self) -> Vec<SupervisorMessage> {
        get_files_in_subfolders_sorted_by_name(
            &self.root,
            ERROR_FOLDER.to_string(),
            YAML_EXTENSION.to_string(),
        )
        .unwrap_or_else(|_| Vec::new())
        .into_iter()
        .filter_map(|file| {
            match SupervisorMessage::try_from((file.clone(), PayloadType::Request)) {
                Ok(msg) => Some(msg),
                Err(e) => {
                    error!(
                        "Failed to extract message from error file {:?}: {:?}",
                        file, e
                    );
                    None
                }
            }
        })
        .collect()
    }

    pub fn at_queued(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::change_to(message, QUEUED_FOLDER.to_string())
    }

    pub fn at_ongoing(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::change_to(message, ONGOING_FOLDER.to_string())
    }

    pub fn at_complete(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::change_to(message, COMPLETE_FOLDER.to_string())
    }

    pub fn planned(message: SupervisorMessage, name: String) -> std::io::Result<()> {
        let source = message.file();
        let root = message
            .file()
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message parent directory for not found for '{message:?}'"),
                )
            })?
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message grandparent directory not found for '{message:?}'"),
                )
            })?
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message grand grandparent directory not found for '{message:?}'"),
                )
            })?;
        let target = root.join(PLANNED_FOLDER).join(name);
        fs::rename(source, target)?;
        Ok(())
    }

    pub fn queued(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::move_to(
            message,
            PLANNED_FOLDER.to_string(),
            QUEUED_FOLDER.to_string(),
        )
    }

    pub fn ongoing(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::move_to(
            message,
            QUEUED_FOLDER.to_string(),
            ONGOING_FOLDER.to_string(),
        )
    }

    pub fn complete(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::move_to(
            message,
            ONGOING_FOLDER.to_string(),
            COMPLETE_FOLDER.to_string(),
        )
    }

    pub fn error(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        for folder in FOLDERS {
            let source = Self::locate_at(message.clone(), folder.to_string())?;
            if source.is_some() {
                let target = Self::at_error(source.clone().unwrap().clone())?;
                debug!(
                    "Moving error message from '{:?}' to '{:?}'",
                    source.clone().unwrap(),
                    target
                );
                fs::rename(source.unwrap().file(), target.file())?;
                return Ok(target);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Message couldn't be moved to the to error folder '{message:?}'"),
        ))
    }

    pub fn fail(message: SupervisorMessage) -> std::io::Result<()> {
        let source = message.file();
        let root = message
            .file()
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message parent directory not found for '{message:?}'"),
                )
            })?
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message grandparent directory not found for '{message:?}'"),
                )
            })?;
        let target = root
            .join(FAIL_FOLDER)
            .join(message.file().file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message file name not found for '{message:?}'"),
                )
            })?);
        fs::rename(source, target)?;
        Ok(())
    }

    fn change_to(message: SupervisorMessage, vault: String) -> std::io::Result<SupervisorMessage> {
        let root = message
            .file()
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Parent directory not found for '{message:?}'"),
                )
            })?
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Grandparent directory not found for '{message:?}'"),
                )
            })?;
        let target = root
            .join(vault)
            .join(message.file().file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File name not found for '{message:?}'"),
                )
            })?);
        let mut message = message.clone();
        message.set_file(target);
        Ok(message)
    }

    fn locate_at(
        message: SupervisorMessage,
        vault: String,
    ) -> std::io::Result<Option<SupervisorMessage>> {
        let located_message = Self::change_to(message, vault)?;
        Ok(if located_message.file().exists() {
            Some(located_message)
        } else {
            None
        })
    }

    fn at_error(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        if let Some(folder) = message.file().parent() {
            if let Some(file) = message.file().file_name() {
                let mut path = PathBuf::from(folder);
                path.push(ERROR_FOLDER);
                path.push(file);
                let mut message = message.clone();
                message.set_file(path);
                return Ok(message);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Error relocating message to error folder '{message:?}'"),
        ))
    }

    fn move_to(
        message: SupervisorMessage,
        source_vault: String,
        target_vault: String,
    ) -> std::io::Result<SupervisorMessage> {
        let source = Self::change_to(message.clone(), source_vault)?;
        let target = Self::change_to(message.clone(), target_vault)?;
        fs::rename(source.file(), target.file())?;
        Ok(target)
    }
}
