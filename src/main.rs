//! Find first unique word from a large CSV file.
//!
//! ## Problem Description
//!
//! Given a large CSV file (~100GB) and a limited RAM machine (16GB),
//! find the first unique word from the CSV file by single iteration.
//!
//! ## Optimization Goal
//!
//! * Eliminating I/O operations (number of reads, writes).
//!

use env_logger;
use log;

fn main() {
    env_logger::init();

    log::info!("Start finding the first unique word from CSV file.");
}
