//! Read the CSV file into memory.

use crate::entry::Record;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};

const DEFAULT_BUFFER_SIZE: u64 = 1024 * 1024 * 1024;

const BUFFER_FACTOR: f64 = 1.0 / 8.0;

pub fn parse(path: &str) -> io::Result<()> {
    let buff_reader = create_buffer_reader(path)?;

    let mut btree = BTreeMap::new();

    // We don't really need to check each size for making it memory safe.
    // In contrast, we drop the in-memory tree map and flush into disk for memory safety.
    for (index, line) in buff_reader.lines().enumerate() {
        match line {
            Ok(value) => {
                // FIXME: the trim can be done only once after finding the unique word
                // to reduce overhead here.
                let s = value.trim_end_matches(",");
                if let Some(record) = btree.get(s) {
                    let new_record = Record::new(index).merge(record);
                    btree.insert(s.to_string(), new_record);
                } else {
                    btree.insert(s.to_string(), Record::new(index));
                }
            }
            Err(err) => return Err(err),
        }
    }

    for (key, record) in btree {
        println!("{}: {}", key, record);
    }
    Ok(())
}

/// Construct buffer reader to enhance read efficiency.
///
/// The default buffer size is 1GB, but can be greater if the file greater than 8GB.
/// Than each buffer size will be FILE_SIZE * BUFFER_FACTOR (1/8).
fn create_buffer_reader(path: &str) -> io::Result<BufReader<File>> {
    let file = File::open(path)?;

    let file_size = file.metadata()?.len();

    let divided_size = (file_size as f64 / BUFFER_FACTOR) as u64;

    let buff_size = if divided_size < DEFAULT_BUFFER_SIZE {
        DEFAULT_BUFFER_SIZE
    } else {
        divided_size
    };

    let buff_reader = BufReader::with_capacity(buff_size as usize, file);

    Ok(buff_reader)
}
