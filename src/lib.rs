//! Find first unique word from a large CSV file.
//!
//! See README.md for algorithm illustration.

use log;

mod csv_parser;
mod entry;

/// Determine which strategy to be used.
#[derive(Debug)]
pub enum Strategy {
    MapReduce(u64),
}

/// Main entry function for finding the first unique value.
pub fn find_first_unique(strategy: Strategy) -> String {
    log::info!("Find the first unique word using strategy: {:?}", strategy);

    match strategy {
        Strategy::MapReduce(level) => String::from("unimpl"),
    }
}
