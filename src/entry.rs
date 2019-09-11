//! Entry module consists of basic abstractions of storage.
//!
//! Including to: `Record`, `Entry` and `Block`.

use std::fmt::{self, Display};
use std::io::Cursor;

use bytes::{self, Buf, BufMut};

/// Storing the metadata for tracking through the algorithm.
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

/// Represents the storage block of each entry.
/// It's a length encoded binary format.
///
/// The storage layout:
///
/// =====================================================================================
/// Offset (8 bytes) | Key (determined by Offset) | Count (8 bytes) | Index (8 bytes)
/// =====================================================================================
///
/// Currently, the Count field is not used since all entries write into temp files are count=1.
/// However, preserve this field for potential pre-reducing optimization.
///
/// The u64 based offset, count and index makes the storage much greater if the keys are small.
/// But it's an acceptable and much scalable than other numeric type.
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
        // Note: the BytesMut is not dynamic resizable.
        // Allocate appropriate capacity for it since we already know that.
        let buf_size = self.offset + 24;
        let mut buf = bytes::BytesMut::with_capacity(buf_size as usize);
        buf.put_u64_be(self.offset);
        buf.put(&self.entry.key);
        buf.put_u64_be(self.entry.record.count);
        buf.put_u64_be(self.entry.record.index);
        buf.take()
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
        let record0 = Record::new(0);
        let record1 = Record::new(1);

        assert_eq!(record0.merge(&record1), Record { count: 2, index: 0 });
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
