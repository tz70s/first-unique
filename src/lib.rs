//! Find first unique word from a large CSV file.
//!
//! See README.md for algorithm illustration.

use std::fs::File;

use log;

mod chunk;
mod entry;
mod reduce;
mod shuffle;

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

    let file = File::open(conf.source_csv).expect("Failed to open file.");

    let result = shuffle::Group::run(file).and_then(|group| {
        let key = reduce::Reducer::for_first_unique(group);

        group.remove_temp_files().map(|_| key)
    });

    match result {
        Ok(key) => key,
        Err(error) => {
            log::error!("Failed to find unique key, cause: {}", error);
            None
        }
    }
}
