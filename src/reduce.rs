//! Reducer perform two phases of reduction.
//!
//! First, for each local partition, translate entries into word count based hash map,
//! and find the earliest (by index).
//!
//! Second, merge those earliest entry and find the earliest entry.

use std::collections::HashMap;
use std::fs;
use std::io::Read;

use log;

use crate::entry;
use crate::entry::{Block, Entry, Record};
use crate::shuffle;

pub struct Reducer {
    group: shuffle::Group,
}

impl Reducer {
    pub fn for_first_unique(group: shuffle::Group) -> Option<String> {
        log::info!("Start reducing process.");

        let reducer = Reducer { group };

        reducer.reduce_global_unique()
    }

    fn reduce_global_unique(&self) -> Option<String> {
        let mut entries = Vec::new();

        let thread_nums = self.group.threads() / 2;

        let thread_groups = self.group.size() / thread_nums;

        for thread_group in 0..thread_groups {
            let mut handles = vec![];

            for t in 0..thread_nums {
                // TODO: use thread pool?
                let handle = std::thread::spawn(move || {
                    let tmp_file = format!(
                        "{}{}",
                        shuffle::TEMP_FILE_PREFIX,
                        thread_group * thread_nums + t
                    );

                    log::debug!("Reduce local file {} to find first unique entry.", tmp_file);

                    let mut file =
                        fs::File::open(tmp_file).expect("Failed to open temporary file.");

                    reduce_local_unique(&mut file)
                });

                handles.push(handle);
            }

            for handle in handles {
                match handle.join().unwrap() {
                    Some(entry) => entries.push(entry),
                    None => (),
                }
            }
        }

        self.find_first_word_from_reduced_entries(entries)
    }

    /// Find the word with minimum index value from a list of entries.
    fn find_first_word_from_reduced_entries(&self, entries: Vec<Entry>) -> Option<String> {
        let (result, _) =
            entries
                .into_iter()
                .fold((None, u64::max_value()), |(origin, min_index), entry| {
                    if entry.record.index < min_index {
                        (Some(entry.key), entry.record.index)
                    } else {
                        (origin, min_index)
                    }
                });

        result
    }
}

fn reduce_local_unique<R: Read>(reader: &mut R) -> Option<entry::Entry> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();
    buf.shrink_to_fit();

    let entries = Block::parse_entries(buf);

    let mut merged_map = HashMap::new();

    for entry in entries {
        if let Some(old_record) = merged_map.get_mut(&entry.key) {
            let new_record = entry.record.merge(old_record);
            *old_record = new_record;
        } else {
            merged_map.insert(entry.key, entry.record);
        }
    }

    merged_map.shrink_to_fit();
    find_first_entry_from_reduced_map(merged_map)
}

/// Find the unique (count == 1) word with minimum index from a given word count hash map.
fn find_first_entry_from_reduced_map(merged_map: HashMap<String, Record>) -> Option<Entry> {
    let (result, _) = merged_map.into_iter().fold(
        (None, u64::max_value()),
        |(origin, min_index), (key, record)| {
            if record.count == 1 && record.index < min_index {
                let replaced_index = record.index;
                let entry = entry::Entry::from_record(key.to_string(), record);
                (Some(entry), replaced_index)
            } else {
                (origin, min_index)
            }
        },
    );
    result
}
