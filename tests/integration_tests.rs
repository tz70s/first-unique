//! Integration test.

use failure::Error;

mod csv_generator;

#[test]
fn test_one_unique_word_csv_file() {
    let result = first_unique::find_first_unique("data/test.csv");

    assert_eq!(Some("apple".to_string()), result);
}

#[test]
#[ignore]
fn test_from_large_csv_file() -> Result<(), Error> {
    csv_generator::create_test_csv("data/test_large.csv")?;

    let result = first_unique::find_first_unique("data/test_large.csv");

    Ok(())
}
