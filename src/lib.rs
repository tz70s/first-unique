//! Find first unique word from a large CSV file.
//!
//! See README.md for algorithm illustration.

use log;
use std::fs::File;

mod buff_io;
mod entry;
mod map_reduce;

pub struct Config<'a> {
    source_csv: &'a str,
}

impl Config<'_> {
    pub fn new(source_csv: &str) -> Config {
        Config { source_csv }
    }
}

/// Main entry function for finding the first unique value.
pub fn find_first_unique(conf: Config) -> Option<String> {
    log::info!("Find the first unique word for file {}", conf.source_csv);

    let mr = map_reduce::MapReduce::new(4);

    let file = File::open(conf.source_csv).expect("Failed to open file.");

    mr.group_by_hash(file);

    mr.reduce_unique()
}
