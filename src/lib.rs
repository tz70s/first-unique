//! Find first unique word from a large CSV file.
//!
//! See README.md for algorithm illustration.

use std::fs::File;
use std::path::Path;

use failure::{Error, ResultExt};
use log;

mod entry;
mod reduce;
mod shuffle;

/// Main entry function for finding the first unique value.
pub fn find_first_unique<P: AsRef<Path>>(csv_source: P) -> Option<String> {
    let csv_source = csv_source.as_ref();

    log::info!(
        "Find the first unique word for file {}",
        csv_source.display()
    );

    match internal_process(csv_source) {
        Ok(key) => key,
        Err(error) => {
            log::error!("Failed to find unique key, cause: {}", error);
            None
        }
    }
}

fn internal_process<P: AsRef<Path>>(csv_source: P) -> Result<Option<String>, Error> {
    let file = File::open(csv_source).context("Missing source csv file")?;

    shuffle::Group::run(file).and_then(|group| {
        let key = reduce::Reducer::for_first_unique(group);
        group.remove_temp_files().map(|_| key)
    })
}
