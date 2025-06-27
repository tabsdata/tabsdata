//
// Copyright 2024 Tabs Data Inc.
//

use crate::transporter::copy::Message;
use std::ops::Range;
use td_error::td_error;
use tokio::sync::mpsc::error::SendError;

pub fn range_to_string(range: &Range<usize>) -> String {
    format!("{}..{}", range.start, range.end)
}

#[td_error]
pub enum TransporterError {
    #[error("The request file '{0}' does not exist")]
    RequestFileNotFound(String) = 0,
    #[error("The directory for the report file '{0}' does not exist")]
    ReportFileDirNotFound(String) = 1,
    #[error("The report file '{0}' must not exist")]
    ReportFileMustNotExist(String) = 2,
    #[error("The request file '{0}' could not be read, error: {1}")]
    CouldNotReadRequest(String, serde_yaml::Error) = 3,
    #[error("The request path '{0}' cannot be opened as a file, error: {1}")]
    RequestFileCannotBeOpened(String, std::io::Error) = 4,
    #[error("The report file '{0}' cannot be created, error: {1}")]
    ReportFileCannotBeCreated(String, std::io::Error) = 5,
    #[error("The report could not be written to '{0}', error: {1}")]
    CouldNotWriteReport(String, serde_yaml::Error) = 6,
    #[error("The environment variable '{0}' is not set")]
    EnvironmentVariableNotFound(String) = 7,

    #[error("Could not encode '{0}' info: {0}")]
    CouldNotEncodeInfo(String, String) = 8,
    #[error("Could not decode '{0}' info: {0}")]
    CouldNotDecodeInfo(String, String) = 9,
    #[error("Importer file URL does not have a path or file name: {0}")]
    InvalidImporterFileUrl(String) = 10,
    #[error("The URL patter '{0}' and the File '{1}' have different base paths")]
    UrlPatternAndFileHaveDifferentBasePaths(String, String) = 11,

    #[error("Could not list files at '{0}' to import, error: {1}")]
    CouldListFilesToImport(String, object_store::Error) = 12,

    #[error("Could not create import instructions: {0}")]
    CouldNotCreateImportInstructions(String) = 13,

    #[error("Could not create object store for '{0}', error: {1}")]
    CouldNotCreateObjectStore(String, object_store::Error) = 5000,
    #[error("Could not get file metadata for '{0}', error: {1}")]
    CouldNotGetFileMetadata(String, Box<object_store::Error>) = 5001,
    #[error("Could not get file metadata for '{0}', error: {1}")]
    CouldNotGetFileRange(String, String, Box<object_store::Error>) = 5002,
    #[error("Could create multipart for '{0}', error: {1}")]
    CouldNotCreateMultipart(String, Box<object_store::Error>) = 5003,
    #[error("Could not complete multipart upload for '{0}', error: {1}")]
    CouldNotCompleteMultipartUpload(String, Box<object_store::Error>) = 5004,
    #[error("Could not send data block for '{0}', error: {1}")]
    CouldNotSendBlock(String, String, SendError<Message>) = 5005,
}
