//! Read the CSV file into memory.

use std::fs::File;
use std::io;

const DEFAULT_BUFFER_SIZE: u64 = 1024 * 1024 * 1024;

const BUFFER_FACTOR: f64 = 1.0 / 8.0;

/// The default buffer size is 1GB, but can be greater if the file greater than 8GB.
/// Than each buffer size will be FILE_SIZE * BUFFER_FACTOR (1/8).
pub fn chunk_size(file: File) -> io::Result<u64> {
    let file_size = file.metadata()?.len();

    let divided_size = (file_size as f64 / BUFFER_FACTOR) as u64;

    let chunk_size = if divided_size < DEFAULT_BUFFER_SIZE {
        DEFAULT_BUFFER_SIZE
    } else {
        divided_size
    };

    Ok(chunk_size)
}
