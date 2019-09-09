//! Integration test.

use std::error::Error;

mod csv_generator;

#[test]
#[ignore]
fn test_from_large_csv_file() -> Result<(), Box<dyn Error>> {
    csv_generator::create_test_csv("data/test_large.csv")?;

    Ok(())
}
