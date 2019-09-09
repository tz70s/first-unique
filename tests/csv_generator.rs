//! Generate large csv file.
//!
//! Execution:
//! ```
//! cargo run --bin csv_generator
//! ```

use rand::{self, Rng};
use std::fs::File;
use std::io::{BufWriter, Write};

const NUMBER_OF_GBS: usize = 1;

// Generate random string in alphanumeric distributions ([a-zA-Z0-9]*) in a given length.
#[inline]
fn random_string(length: usize) -> String {
    let rand_string: String = rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(length)
        .collect();

    rand_string
}

pub fn create_test_csv(location: &str) -> Result<(), std::io::Error> {
    let path = std::path::Path::new(location);

    if !path.exists() {
        let file = File::create(path)?;
        // Use default 8KB buffer size.
        let mut buff_writer = BufWriter::new(file);

        // Assume that each random value consists of 12 bytes.
        let runs = NUMBER_OF_GBS * 1024 * 1024 * 85;

        for _ in 0..runs {
            let mut value = random_string(10);
            value += ",\n";

            buff_writer.write(value.as_bytes())?;
        }
    }

    Ok(())
}
