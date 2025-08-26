//
// Copyright 2025 Tabs Data Inc.
//

use bytes::Bytes;
use futures_util::FutureExt;
use std::path::PathBuf;
use td_common::server::SSL_CERT_PEM_FILE;
use td_error::{TdError, td_error};
use td_objects::types::stream::BoxedSyncStream;
use td_tower::extractors::SrvCtx;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[td_error]
enum CertDownloadError {
    #[error("Certificate PEM file not found in server")]
    NotFound = 0,
    #[error("Error opening certificate PEM file: {0}")]
    Open(std::io::Error) = 1,
    #[error("Error reading certificate PEM file: {0}")]
    Read(std::io::Error) = 5001,
}

pub async fn get_certificate_pem_file(
    SrvCtx(ssl_folder): SrvCtx<PathBuf>,
) -> Result<BoxedSyncStream, TdError> {
    let cert_file = ssl_folder.join(SSL_CERT_PEM_FILE);
    if !cert_file.exists() {
        Err(CertDownloadError::NotFound)?
    }

    let file = File::open(cert_file)
        .await
        .map_err(CertDownloadError::Open)?;

    let stream = async move {
        let mut bytes = Vec::new();
        let mut file = file;
        file.read_to_end(&mut bytes)
            .await
            .map_err(CertDownloadError::Read)?;
        Ok(Bytes::from(bytes))
    }
    .into_stream();

    Ok(BoxedSyncStream::new(stream))
}
