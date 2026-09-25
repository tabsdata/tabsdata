//
// Copyright 2025 Tabsdata Inc.
//

use crate::transporter::common::{parse_store, tweak_store};
use crate::transporter::error::TransporterError;
use crate::transporter::retry::with_retry;
use bytes::Bytes;
use object_store::path::{Path, Path as ObjectPath};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use polars::prelude::{PolarsError, PolarsResult, SinkTarget, SpecialEq};
use polars_io::utils::file::{Writable, WritableTrait};
use std::collections::HashMap;
use std::io;
use std::io::{Error, Result as IoResult, Write};
use std::sync::{Arc, Mutex};
use url::Url;

pub struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    buffer: Vec<u8>,
}

impl ObjectStoreWriter {
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjectPath) -> Self {
        Self {
            store,
            path,
            buffer: Vec::new(),
        }
    }

    pub fn flush_to_store(&mut self) -> IoResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = Bytes::from(std::mem::take(&mut self.buffer));
        let store = Arc::clone(&self.store);
        let path = self.path.clone();

        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(async move {
                with_retry("cloud_put", || async {
                    store.put(&path, PutPayload::from_bytes(data.clone())).await
                })
                .await
            });
            if let Err(e) = tx.send(result) {
                tracing::warn!(
                    channel = "cloud.put_result",
                    "shuttle: send dropped ({:?})",
                    e
                );
            }
        });

        rx.recv()
            .map_err(|_| Error::other("Channel receive failed"))?
            .map_err(Error::other)?;

        Ok(())
    }
}

impl Write for ObjectStoreWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Drop for ObjectStoreWriter {
    fn drop(&mut self) {
        let _ = self.flush_to_store();
    }
}

impl WritableTrait for ObjectStoreWriter {
    fn close(&mut self) -> io::Result<()> {
        self.flush_to_store()
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
}

pub fn create_sink_target(
    url: &Url,
    cloud_config: &HashMap<String, String>,
) -> PolarsResult<SinkTarget> {
    let (store, path) = create_store(url, cloud_config)
        .map_err(|e| PolarsError::ComputeError(format!("Failed to parse URL: {e}").into()))?;
    let writer = Writable::Dyn(Box::new(ObjectStoreWriter::new(Arc::from(store), path)));
    let target = SinkTarget::Dyn(SpecialEq::new(Arc::new(Mutex::new(Some(writer)))));
    Ok(target)
}

fn create_store(
    url: &Url,
    cloud_configs: &HashMap<String, String>,
) -> Result<(Box<dyn ObjectStore>, Path), TransporterError> {
    let (store, path) = parse_store(url, cloud_configs)?;
    let path = tweak_store(url, &path);
    Ok((store, path))
}
