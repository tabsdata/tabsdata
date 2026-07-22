//
// Copyright 2026 Tabsdata Inc.
//

//! The `ByteCopy` primitive: move an object's bytes from source to target with
//! no format decode/encode — optionally transcoding the *compression* codec
//! (decompress on read, recompress on write). Used for passthrough copies and
//! as the download/upload legs of staging.
//!
//! Streams source → target with bounded memory: a streaming reader is piped
//! into the target's multipart writer, so an arbitrarily large object copies
//! without being held in memory, and the target object appears only on the
//! atomic multipart `complete` (a partial output is never exposed).

use crate::transporter::compression::{async_reader, async_writer};
use crate::transporter2::codec;
use crate::transporter2::error::{Result, TransporterError};
use crate::transporter2::io::Store;
use crate::transporter2::spec::Codec;
use tokio::io::AsyncWriteExt;

pub struct ByteCopyJob {
    pub source: Store,
    pub target: Store,
    /// Decompress the source bytes on read (`Codec::None` = as-is).
    pub decompress: Codec,
    /// Compress the target bytes on write (`Codec::None` = as-is).
    pub compress: Codec,
}

pub struct ByteCopyOutcome {
    pub bytes_written: u64,
}

/// Stream-copy one object (source → [decompress] → [compress] → target),
/// returning the number of bytes written to the target.
pub async fn byte_copy_one(job: ByteCopyJob) -> Result<ByteCopyOutcome> {
    let reader = job.source.open_read_stream().await?;
    // Decompress wrapper (a no-op for `Codec::None`). `algo` is a local that
    // outlives the in-scope reader, so the generic wrapper's lifetime quirk
    // (see codec module) does not apply.
    let dec = Some(codec::to_algorithm(job.decompress));
    let reader = async_reader(&dec, "", reader);
    tokio::pin!(reader);

    let writer = job.target.open_write();
    let enc = Some(codec::to_algorithm(job.compress));
    let writer = async_writer(&enc, "", writer);
    tokio::pin!(writer);

    tokio::io::copy(&mut reader, &mut writer)
        .await
        .map_err(TransporterError::from)?;
    writer.shutdown().await.map_err(TransporterError::from)?; // completes multipart

    // Report the actual bytes landed at the target (post-compression).
    let bytes_written = job.target.head().await?.0;
    Ok(ByteCopyOutcome { bytes_written })
}
