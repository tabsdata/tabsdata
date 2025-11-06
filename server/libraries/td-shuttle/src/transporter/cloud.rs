//
// Copyright 2025 Tabs Data Inc.
//

use crate::transporter::common::{parse_store, tweak_store};
use crate::transporter::error::TransporterError;
use bytes::Bytes;
use object_store::path::{Path, Path as ObjectPath};
use object_store::{ObjectStore, PutPayload};
use polars::prelude::{PlPath, PolarsError, PolarsResult, SinkTarget, SpecialEq};
use polars_io::prelude::sync_on_close::SyncOnCloseType;
use polars_io::utils::file::DynWriteable;
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
            let result =
                rt.block_on(async move { store.put(&path, PutPayload::from_bytes(data)).await });
            let _ = tx.send(result);
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

impl DynWriteable for ObjectStoreWriter {
    fn as_dyn_write(&self) -> &(dyn Write + Send + 'static) {
        self
    }

    fn as_mut_dyn_write(&mut self) -> &mut (dyn Write + Send + 'static) {
        self
    }

    fn close(mut self: Box<Self>) -> io::Result<()> {
        self.flush_to_store()
    }

    fn sync_on_close(&mut self, sync_on_close: SyncOnCloseType) -> io::Result<()> {
        match sync_on_close {
            SyncOnCloseType::None => Ok(()),
            SyncOnCloseType::Data | SyncOnCloseType::All => self.flush_to_store(),
        }
    }
}

pub fn create_sink_target(
    url: &Url,
    cloud_config: &HashMap<String, String>,
) -> PolarsResult<SinkTarget> {
    match url.scheme() {
        "file" => create_local_sink_target(url),
        _ => create_cloud_sink_target(url, cloud_config),
    }
}

fn create_local_sink_target(url: &Url) -> PolarsResult<SinkTarget> {
    let target = if url.scheme() == "file" {
        #[cfg(not(windows))]
        {
            SinkTarget::Path(PlPath::new(url.path().to_string().as_str()))
        }
        #[cfg(windows)]
        {
            let mut path_str = url.path().to_string();
            if path_str.starts_with('/')
                && path_str.len() > 1
                && path_str.chars().nth(2) == Some(':')
            {
                path_str.remove(0);
            }
            path_str = path_str.replace("/", "\\");
            SinkTarget::Path(PlPath::new(path_str.as_str()))
        }
    } else {
        SinkTarget::Path(PlPath::new(url.as_str()))
    };
    Ok(target)
}

fn create_cloud_sink_target(
    url: &Url,
    cloud_config: &HashMap<String, String>,
) -> PolarsResult<SinkTarget> {
    let (store, path) = create_store(url, cloud_config)
        .map_err(|e| PolarsError::ComputeError(format!("Failed to parse cloud URL: {e}").into()))?;
    let writer: Box<dyn DynWriteable> = Box::new(ObjectStoreWriter::new(Arc::from(store), path));
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
