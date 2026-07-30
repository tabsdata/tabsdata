//
// Copyright 2026 Tabsdata Inc.
//

use std::fs::File;
use std::path::Path;

use td_error::TdError;

use crate::base::constants::{FOLDER_KENVS, FOLDER_PYTHON};
use crate::base::error::provider::ProviderError;

const COMPRESSION_LEVEL: i32 = 3;
const LONG_DISTANCE_WINDOW_LOG: u32 = 27;

pub fn write_venvs_image(uv_home: &Path, image: &Path) -> Result<(), TdError> {
    write_image(uv_home, image).map_err(|cause| {
        ProviderError::VenvsImageWriteFailed {
            folder: uv_home.display().to_string(),
            image: image.display().to_string(),
            cause,
        }
        .into()
    })
}

fn write_image(uv_home: &Path, image: &Path) -> Result<(), String> {
    if let Some(parent) = image.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    if image.exists() {
        std::fs::remove_file(image).map_err(|error| error.to_string())?;
    }
    let file = File::create(image).map_err(|error| error.to_string())?;
    let mut encoder =
        zstd::Encoder::new(file, COMPRESSION_LEVEL).map_err(|error| error.to_string())?;
    encoder
        .multithread(std::thread::available_parallelism().map_or(1, |count| count.get() as u32))
        .map_err(|error| error.to_string())?;
    encoder
        .long_distance_matching(true)
        .map_err(|error| error.to_string())?;
    encoder
        .window_log(LONG_DISTANCE_WINDOW_LOG)
        .map_err(|error| error.to_string())?;
    encoder
        .include_contentsize(true)
        .map_err(|error| error.to_string())?;
    {
        let mut builder = tar::Builder::new(&mut encoder);
        builder.follow_symlinks(false);
        for folder in [FOLDER_PYTHON, FOLDER_KENVS] {
            let source = uv_home.join(folder);
            if !source.is_dir() {
                continue;
            }
            builder
                .append_dir_all(folder, &source)
                .map_err(|error| error.to_string())?;
        }
        builder.finish().map_err(|error| error.to_string())?;
    }
    encoder.finish().map_err(|error| error.to_string())?;
    Ok(())
}
