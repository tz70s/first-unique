//! Entry is the basic abstraction of storage entry.

use std::fmt::{self, Display};
use std::io::Cursor;

use bytes::{self, Buf, BufMut};

#[derive(Debug, Clone, Eq, PartialEq)]
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

#[derive(Debug, Clone, Eq, PartialEq)]
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
/// =====================================================================================
/// Offset (8 bytes) | Value (determined by Key size) | Count (8 bytes) | Index (8 bytes)
/// =====================================================================================
pub struct Block {
    offset: u64,
    entry: Entry,
}

impl Block {
    pub fn create(key: String, index: usize) -> Block {
        let entry = Entry::new(key, index);
        let offset = entry.key.as_bytes().len() as u64;
        Block { offset, entry }
    }

    pub fn as_bytes(&self) -> bytes::BytesMut {
        let mut buf = bytes::BytesMut::new();
        buf.put_u64_be(self.offset);
        buf.put(&self.entry.key);
        buf.put_u64_be(self.entry.record.count);
        buf.put_u64_be(self.entry.record.index);
        buf.take()
    }

    #[inline]
    pub fn entry(self) -> Entry {
        self.entry
    }

    pub fn parse_entries(bytes: &[u8]) -> Vec<Entry> {
        let mut entries = vec![];
        let mut buf = Cursor::new(bytes);

        // FIXME: should carefully consider the error path and corner case.

        while buf.has_remaining() {
            let offset = buf.get_u64_be() as usize;

            let key = std::str::from_utf8(buf.by_ref().take(offset).bytes())
                .unwrap()
                .to_string();

            buf.advance(offset);

            let count = buf.get_u64_be();
            let index = buf.get_u64_be();

            let entry = Entry::new(key, index as usize);
            entries.push(entry);
        }

        entries
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
        expect.extend(&[0, 0, 0, 0, 0, 0, 0, 0]);

        assert_eq!(bytes, &expect[..]);
    }

    #[test]
    fn test_block_deserialise() {
        let block0 = Block::create("Hello".to_string(), 0);
        let block1 = Block::create("World".to_string(), 1);

        let mut bytes = block0.as_bytes();
        bytes.extend(block1.as_bytes());

        let bytes_slice: &[u8] = &bytes;

        let expect = vec![
            Entry::new("Hello".to_string(), 0),
            Entry::new("World".to_string(), 1),
        ];

        let entries = Block::parse_entries(bytes_slice);

        assert_eq!(entries, expect);
    }
}
