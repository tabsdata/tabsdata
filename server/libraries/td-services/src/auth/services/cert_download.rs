//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::cert_download::get_certificate_pem_file;
use std::path::PathBuf;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_error::TdError;
use td_objects::types::stream::BoxedSyncStream;

#[service_factory]
async fn cert_download(ssl_folder: Ctx<PathBuf>, _request: ()) -> Result<BoxedSyncStream, TdError> {
    get_certificate_pem_file(&ssl_folder).await
}

#[cfg(test)]
mod tests {
    use crate::Services;

    use futures_util::StreamExt;
    use ta_services::service::Ctx;
    use td_common::crypto::SSL_CERT_PEM_FILE;

    use td_error::TdError;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_cert_download_ok() -> Result<(), TdError> {
        let tls_path = td_common::tdtestdir!();
        fs::create_dir_all(&tls_path).await.unwrap();

        let key_path = tls_path.join(SSL_CERT_PEM_FILE);
        fs::File::create(&key_path)
            .await
            .unwrap()
            .write_all("TEST CERTIFICATE".as_ref())
            .await
            .unwrap();

        let service = Services {
            ssl_folder: Ctx::new(tls_path),
            ..Services::default()
        };
        let response = service.cert_download(()).await?.data;
        let mut response = response.into_inner();
        let bytes = response.next().await.unwrap()?;

        let cert_content = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(cert_content, "TEST CERTIFICATE");
        Ok(())
    }

    #[tokio::test]
    async fn test_cert_download_err() -> Result<(), TdError> {
        let tls_path = td_common::tdtestdir!();
        let service = Services {
            ssl_folder: Ctx::new(tls_path),
            ..Services::default()
        };
        let response = service.cert_download(()).await;
        assert!(response.is_err());
        Ok(())
    }
}
