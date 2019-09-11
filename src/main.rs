//! Find first unique word from a large CSV file.
//!
//! See README.md for algorithm illustration.

use env_logger;

use first_unique;

fn main() {
    let arg = std::env::args().skip(1).next();

    let file_name = arg.unwrap_or("data/test.csv".to_string());

    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");

    env_logger::init_from_env(env);

    let result = first_unique::find_first_unique(file_name);

    match result {
        Some(value) => log::info!("Found the first unique value : {}", value),
        None => log::info!("Can't find the unique value."),
    }
}
