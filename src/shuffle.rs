//! MapReduce based implementation.
//!
//! The workflow is illustrated as following:
//!
//! The main thread read csv file and group values into different threads.

use std::collections::hash_map;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Read;

use failure::Error;

mod shuffler;

pub const TEMP_FILE_PREFIX: &'static str = "/tmp/word-count";
pub const DEFAULT_GROUP_SIZE: u32 = 32;
pub const DEFAULT_THREADS: u32 = 8;

fn temp_file(index: u32) -> String {
    format!("{}{}", TEMP_FILE_PREFIX, index)
}

#[derive(Clone, Copy)]
pub struct Group {
    size: u32,
    threads: u32,
}

impl Group {
    pub fn run<R: Read>(csv_source: R) -> Result<Group, Error> {
        Group::run_with_group_size(csv_source, DEFAULT_GROUP_SIZE, DEFAULT_THREADS)
    }

    pub fn run_with_group_size<R: Read>(
        csv_source: R,
        group_size: u32,
        threads: u32,
    ) -> Result<Group, Error> {
        let group = Group {
            size: group_size,
            threads,
        };

        let shuffler = shuffler::Shuffler::new(group);
        shuffler.run_partition(csv_source)?;

        Ok(group)
    }

    /// Clean-up temporary files.
    pub fn remove_temp_files(&self) -> Result<(), Error> {
        for index in 0..self.size {
            fs::remove_file(temp_file(index))?;
        }

        Ok(())
    }

    #[inline]
    pub fn size(&self) -> u32 {
        self.size
    }

    #[inline]
    pub fn threads(&self) -> u32 {
        self.threads
    }

    #[inline]
    fn make_index(&self, val: &str) -> u32 {
        // TODO: the hash and modular computation is the most heavy cpu-intensive job.
        // Can we try to accelerate this?
        let mut hasher = hash_map::DefaultHasher::new();

        val.hash(&mut hasher);

        let hash_val = hasher.finish();

        (hash_val % (self.size as u64)) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_index() {
        let group = Group {
            size: 4,
            threads: 1,
        };

        let idx1 = group.make_index("Jon");
        let idx2 = group.make_index("Jon");

        assert_eq!(idx1, idx2);
    }
}
