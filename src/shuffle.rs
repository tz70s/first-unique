//! MapReduce based implementation.
//!
//! The workflow is illustrated as following:
//!
//! The main thread read csv file and group values into different threads.

use std::collections::hash_map;
use std::hash::{Hash, Hasher};
use std::io::{self, Read};

mod shuffler;

pub const TEMP_FILE_PREFIX: &'static str = "/tmp/word-count";

#[derive(Clone, Copy)]
pub struct Group {
    size: u32,
}

impl Group {
    pub fn run<R: Read>(csv_source: R) -> Result<Group, io::Error> {
        Group::run_with_group_size(csv_source, 8)
    }

    pub fn run_with_group_size<R: Read>(
        csv_source: R,
        group_size: u32,
    ) -> Result<Group, io::Error> {
        let group = Group { size: group_size };

        let shuffler = shuffler::Shuffler::new(group);
        shuffler.run_partition(csv_source)?;

        Ok(group)
    }

    #[inline]
    pub fn size(&self) -> u32 {
        self.size
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
        let group = Group { size: 4 };

        let idx1 = group.make_index("Jon");
        let idx2 = group.make_index("Jon");

        assert_eq!(idx1, idx2);
    }
}
