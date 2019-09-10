//! Entry is the basic abstraction of storage entry.

use bytes::{self, BufMut};
use std::fmt::{self, Display};

#[derive(Debug, Clone)]
pub struct Record {
    pub count: u64,
    pub index: u64,
}

impl Record {
    pub fn new(index: u64) -> Record {
        Record { count: 1, index }
    }

    pub fn merge(&self, other: &Record) -> Record {
        let index = if self.index < other.index {
            self.index
        } else {
            other.index
        };

        Record {
            count: self.count + other.count,
            index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub key: String,
    pub record: Record,
}

impl Entry {
    pub fn new(key: String, index: usize) -> Entry {
        let record = Record::new(index as u64);
        Entry { key, record }
    }

    pub fn from_record(key: String, record: Record) -> Entry {
        Entry { key, record }
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}%{}%{}",
            self.key, self.record.count, self.record.index
        )
    }
}

/// The storage layout:
///
/// =====================================================================
/// Key size (8 bytes) | Value (determined by Key size) | Count (8 bytes)
/// =====================================================================
///
pub struct Block {
    key_size: u64,
    entry: Entry,
}

impl Block {
    pub fn from_entry(entry: Entry) -> Block {
        let key_size = entry.key.as_bytes().len() as u64;
        Block { key_size, entry }
    }

    pub fn create(val: String, lineno: usize) -> Block {
        let entry = Entry::new(val, lineno);
        Block::from_entry(entry)
    }

    pub fn as_bytes(&self) -> bytes::BytesMut {
        let mut buf = bytes::BytesMut::new();
        buf.put_u64_be(self.key_size);
        buf.put(&self.entry.key);
        buf.put_u64_be(self.entry.record.count);
        buf.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_merge() {
        let entry1 = Entry::new("Hello".to_string(), 0);
        let entry2 = Entry::new("Hello".to_string(), 1);
        let entry3 = Entry::new("Hello".to_string(), 2);
    }

    #[test]
    fn test_block_serialize() {
        let block = Block::create("Hello".to_string(), 0);

        let bytes: &[u8] = &block.as_bytes();

        // Note: the encoding use Big Endian.
        let mut expect: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 5];

        expect.extend(b"Hello");
        expect.extend(&[0, 0, 0, 0, 0, 0, 0, 1]);

        assert_eq!(bytes, &expect[..]);
    }
}
