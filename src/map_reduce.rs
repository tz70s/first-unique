//! MapReduce based implementation.
//!
//! The workflow is illustrated as following:
//!
//! The main thread read csv file and group values into different threads.

use crate::entry;
use log;
use std::collections::{hash_map, HashMap};
use std::error::Error;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::mpsc;
use std::thread;

const TEMP_FILE_PREFIX: &'static str = "/tmp/word-count";

struct Mapper(mpsc::Sender<(String, usize)>, thread::JoinHandle<()>);

pub struct MapReduce {
    parallelism: u32,
}

impl MapReduce {
    pub fn new(parallelism: u32) -> MapReduce {
        MapReduce { parallelism }
    }

    pub fn group_by_hash<R: Read>(&self, csv_source: R) -> Result<(), impl Error> {
        let buff_reader = BufReader::new(csv_source);

        let mappers = self.spawn_mappers();

        for (lineno, line) in buff_reader.lines().enumerate() {
            match line {
                Ok(value) => {
                    // Note: the value here contains a ',' at the end.
                    // We don't need to do early trim to reduce overhead.
                    // But still need to reduce it while finding the target value.
                    let index = make_index(&value, self.parallelism) as usize;

                    log::trace!("Send value {} to thread index {}", value, index);

                    // FIXME: resolve error path.
                    mappers[index].0.send((value, lineno));
                }
                Err(err) => return Err(err),
            }
        }

        // FIXME: deadlock.
        // for mapper in mappers {
        //    mapper.1.join().unwrap();
        // }

        Ok(())
    }

    fn spawn_mappers(&self) -> Vec<Mapper> {
        (0..self.parallelism)
            .into_iter()
            .map(|idx| {
                let (tx, rx) = mpsc::channel();

                let tmp_file = format!("{}{}", TEMP_FILE_PREFIX, idx);

                log::debug!(
                    "Create temp file {} in entry format for future reducing.",
                    tmp_file
                );

                let file = fs::File::create(tmp_file).expect("Failed to create temporary file.");

                let handle = thread::spawn(move || entry_writer(file, rx));

                Mapper(tx, handle)
            })
            .collect()
    }

    pub fn reduce_unique(&self) -> Option<String> {
        let mut entries = Vec::new();

        log::info!("Start reducing process.");

        for index in 0..self.parallelism {
            let tmp_file = format!("{}{}", TEMP_FILE_PREFIX, index);

            log::debug!("Reduce local file {} to find first unique entry.", tmp_file);

            let file = fs::File::open(tmp_file).expect("Failed to open temporary file.");

            match self.reduce_local_unique(file) {
                Some(entry) => entries.push(entry),
                None => (),
            }
        }

        let mut result = None;
        let mut min_lineno = u64::max_value();

        for entry in entries {
            if entry.record.lineno < min_lineno {
                min_lineno = entry.record.lineno;
                result = Some(entry.key)
            }
        }

        result.map(|text| text.trim_end_matches(',').to_string())
    }

    fn reduce_local_unique<R: Read>(&self, reader: R) -> Option<entry::Entry> {
        let buff_reader = BufReader::new(reader);

        let merged_map = buff_reader
            .lines()
            .map(|res| {
                let text = res.unwrap();
                let entry_fields: Vec<_> = text.split('%').collect();
                entry::Entry::new(
                    entry_fields[0].to_string(),
                    entry_fields[2].parse().unwrap(),
                )
            })
            .fold(HashMap::new(), |mut acc, entry| {
                if let Some(old_record) = acc.get(&entry.key) {
                    let new_record = entry.record.merge(old_record);
                    acc.insert(entry.key, new_record);
                } else {
                    acc.insert(entry.key, entry.record);
                }
                acc
            });

        let mut result = None;
        let mut min_lineno = u64::max_value();

        for (val, record) in merged_map {
            if record.count == 1 && record.lineno < min_lineno {
                min_lineno = record.lineno;
                result = Some(entry::Entry::from_record(val, record));
            }
        }

        result
    }
}

#[inline]
fn make_index(val: &str, nr_groups: u32) -> u32 {
    // TODO: the hash and modular computation is the most heavy cpu-intensive job.
    // Can we try to accelerate this?
    let mut hasher = hash_map::DefaultHasher::new();

    val.hash(&mut hasher);

    let hash_val = hasher.finish();

    (hash_val % (nr_groups as u64)) as u32
}

fn entry_writer<W: Write>(target: W, rx: mpsc::Receiver<(String, usize)>) {
    let mut writer = BufWriter::new(target);

    for (val, lineno) in rx {
        let ent = entry::Entry::new(val, lineno);
        let text = format!("{}\n", ent);

        writer
            .write(text.as_bytes())
            .expect("Unexpected write failure");
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_make_index() {
        let idx1 = make_index("Jon", 10);
        let idx2 = make_index("Jon", 10);
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn test_entry_writer() {
        let (tx, rx) = mpsc::channel();
        let source = vec!["Hello", "World", "This", "Is", "The", "Test", "Data"];
        let source_clone = source.clone();

        let _ = thread::spawn(move || {
            for (idx, val) in source_clone.into_iter().enumerate() {
                tx.send((val.to_string(), idx)).unwrap();
            }
        });

        let mut entries = vec![];
        entry_writer(&mut entries, rx);

        /*
        let text = std::str::from_utf8(&entries).unwrap();

        let expect = source
            .into_iter()
            .map(|text| format!("{}\n", Entry::new(text.to_string())))
            .fold(String::new(), |acc, x| acc + &x);

        assert_eq!(text, expect);

        */
    }
}
