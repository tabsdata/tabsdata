//
// Copyright 2025 Tabs Data Inc.
//

use crate::auth::layers::cert_download::get_certificate_pem_file;
use std::path::PathBuf;
use td_objects::types::stream::BoxedSyncStream;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = CertDownloadService,
    request = (),
    response = BoxedSyncStream,
    context = PathBuf,
)]
fn service() {
    layers!(from_fn(get_certificate_pem_file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::Arc;
    use td_common::server::SSL_CERT_PEM_FILE;
    use td_error::TdError;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;
    use testdir::testdir;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_cert_download() {
        use td_tower::metadata::type_of_val;

        CertDownloadService::with_defaults()
            .metadata()
            .await
            .assert_service::<(), BoxedSyncStream>(&[type_of_val(&get_certificate_pem_file)]);
    }

    #[tokio::test]
    async fn test_cert_download_ok() -> Result<(), TdError> {
        let tls_path = testdir!();
        fs::create_dir_all(&tls_path).await.unwrap();

        let key_path = tls_path.join(SSL_CERT_PEM_FILE);
        fs::File::create(&key_path)
            .await
            .unwrap()
            .write_all("TEST CERTIFICATE".as_ref())
            .await
            .unwrap();

        let service = CertDownloadService::new(Arc::new(tls_path)).service().await;
        let response = service.raw_oneshot(()).await?;
        let mut response = response.into_inner();
        let bytes = response.next().await.unwrap()?;

        let cert_content = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(cert_content, "TEST CERTIFICATE");
        Ok(())
    }

    #[tokio::test]
    async fn test_cert_download_err() -> Result<(), TdError> {
        let tls_path = testdir!();
        let service = CertDownloadService::new(Arc::new(tls_path)).service().await;
        let response = service.raw_oneshot(()).await;
        assert!(response.is_err());
        Ok(())
    }
}
