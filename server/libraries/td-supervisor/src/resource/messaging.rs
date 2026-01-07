//
// Copyright 2024 Tabs Data Inc.
//

use std::fs;
use std::path::PathBuf;
use td_common::files::{
    YAML_EXTENSION, get_files_in_folder_sorted_by_name, get_files_in_subfolders_sorted_by_name,
};
use td_common::server::{
    COMPLETE_FOLDER, ERROR_FOLDER, FAIL_FOLDER, ONGOING_FOLDER, PLANNED_FOLDER, PayloadType,
    QUEUED_FOLDER, STOP_FOLDER, SupervisorMessage,
};
use tracing::{debug, error};

const FOLDERS: [&str; 3] = [PLANNED_FOLDER, QUEUED_FOLDER, ONGOING_FOLDER];

#[derive(Debug)]
pub struct SupervisorMessageQueue {
    root: PathBuf,
}

impl SupervisorMessageQueue {
    pub fn new(root: PathBuf) -> Self {
        let queue = Self { root };
        if let Err(e) = queue.recover_leftover_messages() {
            error!("Failed to recover leftover messages: {:?}", e);
        }
        queue
    }

    fn recover_leftover_messages(&self) -> std::io::Result<()> {
        debug!("Starting recovery of leftover messages from QUEUED and ONGOING folders");

        let ongoing_count = self.recover_leftover_messages_from_folder(ONGOING_FOLDER);
        let queued_count = self.recover_leftover_messages_from_folder(QUEUED_FOLDER);
        let total_count = queued_count + ongoing_count;

        if total_count > 0 {
            debug!(
                "Message recovery complete: {} messages moved to error folders for retry",
                total_count
            );
        }

        Ok(())
    }

    fn recover_leftover_messages_from_folder(&self, folder: &str) -> usize {
        let messages =
            get_files_in_folder_sorted_by_name(self.root.join(folder), Some(YAML_EXTENSION))
                .unwrap_or_else(|_| Vec::new());

        if messages.is_empty() {
            return 0;
        }

        debug!(
            "Found {} leftover messages in {} folder",
            messages.len(),
            folder
        );

        let mut recovered_count = 0;
        for file in messages {
            match SupervisorMessage::try_from((file.clone(), PayloadType::Request)) {
                Ok(message) => {
                    let name = match message.file.file_name().and_then(|name| name.to_str()) {
                        Some(file_name) => file_name.to_string(),
                        None => {
                            error!(
                                "Failed to extract file name from leftover message in {} folder: {:?}",
                                folder, message.file
                            );
                            continue;
                        }
                    };
                    match Self::planned_from_leftover(message.clone(), name) {
                        Ok(_) => {
                            debug!(
                                "Recovered leftover message from {} to planned: {:?}",
                                folder, file
                            );
                            recovered_count += 1;
                        }
                        Err(error) => {
                            error!(
                                "Failed to recover leftover message from {} to planned: {:?} - {:?}",
                                folder, file, error
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to extract leftover message from {} folder: {:?} - {:?}",
                        folder, file, e
                    );
                }
            }
        }

        recovered_count
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

    pub fn planned_from_error(message: SupervisorMessage, name: String) -> std::io::Result<()> {
        let source = &message.file;
        let root = message
            .file
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
        debug!(
            "Renaming message to planned: '{:?}' -> '{:?}'",
            source, target
        );
        match fs::rename(source, &target) {
            Ok(_) => {
                debug!(
                    "Successfully renamed message to planned from error: '{:?}'",
                    target
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to rename message to planned from error: '{:?}' -> '{:?}': {:?}",
                    source, target, e
                );
                Err(e)
            }
        }
    }

    pub fn planned_from_leftover(message: SupervisorMessage, name: String) -> std::io::Result<()> {
        let source = &message.file;
        let root = message
            .file
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
            })?;
        let target = root.join(PLANNED_FOLDER).join(name);
        match fs::rename(source, &target) {
            Ok(_) => {
                debug!(
                    "Successfully renamed message to planned from leftovers: '{:?}'",
                    target
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to rename message to planned from leftovers: '{:?}' -> '{:?}': {:?}",
                    source, target, e
                );
                Err(e)
            }
        }
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

    pub fn finalize(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        Self::move_to(
            message,
            PLANNED_FOLDER.to_string(),
            COMPLETE_FOLDER.to_string(),
        )
    }

    //noinspection DuplicatedCode
    pub fn stop(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        for folder in FOLDERS {
            let source = Self::locate_at(message.clone(), folder.to_string())?;
            if let Some(source) = source {
                let target = Self::at_stop(source.clone())?;
                if let Some(parent) = target.file.parent() {
                    debug!("Creating stop message parent directory: '{:?}'", parent);
                    match fs::create_dir_all(parent) {
                        Ok(_) => {
                            debug!(
                                "Successfully created stop message parent directory: '{:?}'",
                                parent
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to create stop message parent directory '{:?}': {:?}",
                                parent, e
                            );
                            return Err(e);
                        }
                    }
                }
                debug!(
                    "Moving stop message: '{:?}' -> '{:?}'",
                    source.file, target.file
                );
                match fs::rename(&source.file, &target.file) {
                    Ok(_) => {
                        debug!("Successfully moved stop message to: '{:?}'", target.file);
                        return Ok(target);
                    }
                    Err(e) => {
                        error!(
                            "Failed to move stop message '{:?}' -> '{:?}': {:?}",
                            source.file, target.file, e
                        );
                        return Err(e);
                    }
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Message couldn't be moved to the stop folder '{message:?}'"),
        ))
    }

    //noinspection DuplicatedCode
    pub fn error(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        for folder in FOLDERS {
            let source = Self::locate_at(message.clone(), folder.to_string())?;
            if let Some(source) = source {
                let target = Self::at_error(source.clone())?;
                debug!(
                    "Moving error message: '{:?}' -> '{:?}'",
                    source.file, target.file
                );
                match fs::rename(&source.file, &target.file) {
                    Ok(_) => {
                        debug!("Successfully moved error message to: '{:?}'", target.file);
                        return Ok(target);
                    }
                    Err(e) => {
                        error!(
                            "Failed to move error message '{:?}' -> '{:?}': {:?}",
                            source.file, target.file, e
                        );
                        return Err(e);
                    }
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Message couldn't be moved to the error folder '{message:?}'"),
        ))
    }

    pub fn fail(message: SupervisorMessage) -> std::io::Result<()> {
        let source = &message.file;
        let root = message
            .file
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
            .join(message.file.file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Message file name not found for '{message:?}'"),
                )
            })?);
        debug!("Moving message to fail: '{:?}' -> '{:?}'", source, target);
        match fs::rename(source, &target) {
            Ok(_) => {
                debug!("Successfully moved message to fail: '{:?}'", target);
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to move message to fail: '{:?}' -> '{:?}': {:?}",
                    source, target, e
                );
                Err(e)
            }
        }
    }

    fn change_to(message: SupervisorMessage, vault: String) -> std::io::Result<SupervisorMessage> {
        let root = message
            .file
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
            .join(message.file.file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File name not found for '{message:?}'"),
                )
            })?);
        let mut message = message.clone();
        message.file = target;
        Ok(message)
    }

    fn locate_at(
        message: SupervisorMessage,
        vault: String,
    ) -> std::io::Result<Option<SupervisorMessage>> {
        let located_message = Self::change_to(message, vault)?;
        Ok(if located_message.file.exists() {
            Some(located_message)
        } else {
            None
        })
    }

    //noinspection DuplicatedCode
    fn at_stop(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        if let Some(folder) = &message.file.parent()
            && let Some(file) = &message.file.file_name()
        {
            let mut path = PathBuf::from(folder);
            path.push(STOP_FOLDER);
            path.push(file);
            let mut message = message.clone();
            message.file = path;
            return Ok(message);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Error relocating message to stop folder '{message:?}'"),
        ))
    }

    //noinspection DuplicatedCode
    fn at_error(message: SupervisorMessage) -> std::io::Result<SupervisorMessage> {
        if let Some(folder) = &message.file.parent()
            && let Some(file) = &message.file.file_name()
        {
            let mut path = PathBuf::from(folder);
            path.push(ERROR_FOLDER);
            path.push(file);
            let mut message = message.clone();
            message.file = path;
            return Ok(message);
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
        let source = Self::change_to(message.clone(), source_vault.clone())?;
        let target = Self::change_to(message.clone(), target_vault.clone())?;
        debug!(
            "Moving message from '{}' to '{}': '{:?}' -> '{:?}'",
            source_vault, target_vault, source.file, target.file
        );
        match fs::rename(&source.file, &target.file) {
            Ok(_) => {
                debug!(
                    "Successfully moved message from '{}' to '{}': '{:?}'",
                    source_vault, target_vault, target.file
                );
                Ok(target)
            }
            Err(e) => {
                error!(
                    "Failed to move message from '{}' to '{}': '{:?}' -> '{:?}': {:?}",
                    source_vault, target_vault, source.file, target.file, e
                );
                Err(e)
            }
        }
    }
}
