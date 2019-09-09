//! Entry is the basic abstraction of storage entry.

use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub struct Record {
    count: usize,
    index: usize,
}

impl Record {
    pub fn new(index: usize) -> Record {
        Record { count: 1, index }
    }

    pub fn merge(self, other: &Record) -> Record {
        let new_index = if self.index <= other.index {
            self.index
        } else {
            other.index
        };
        Record {
            count: self.count + other.count,
            index: new_index,
        }
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}, {}", self.count, self.index)
    }
}

/// The core data structure of storage abstraction.
/// The keys can be sorted similar to SSTable, and the count and index are used to track numbers and ordering.
/// Due to the ordering from CSV file would be re-ordered.
#[derive(Debug)]
pub struct Entry {
    key: String,
    record: Record,
}

impl Entry {
    pub fn new(key: String, index: usize) -> Entry {
        let record = Record::new(index);
        Entry { key, record }
    }

    pub fn merge(self, other: &Entry) -> Entry {
        let record = self.record.merge(&other.record);
        Entry {
            key: self.key,
            record,
        }
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}, {}", self.key, self.record)
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        self.key == other.key
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Entry) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl Hash for Entry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    #[test]
    fn test_entry_comparison() {
        let entry1 = Entry::new("Hello".to_string(), 0);
        let entry2 = Entry::new("Hello".to_string(), 1);

        assert_eq!(entry1, entry2);

        let entry3 = Entry::new("IceCream".to_string(), 2);
        assert!(entry3 > entry1);
    }

    #[test]
    fn test_entry_merge() {
        let entry1 = Entry::new("Hello".to_string(), 0);
        let entry2 = Entry::new("Hello".to_string(), 1);
        let entry3 = Entry::new("Hello".to_string(), 2);

        let entry4 = entry1.merge(&entry2);
        let entry5 = entry3.merge(&entry4);

        assert_eq!(entry5.key, "Hello".to_string());
        assert_eq!(entry5.record.count, 3);
        assert_eq!(entry5.record.index, 0);
    }
}
