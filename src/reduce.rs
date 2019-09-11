//! Reducer

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

        for index in 0..self.group.size() {
            let tmp_file = format!("{}{}", shuffle::TEMP_FILE_PREFIX, index);

            log::debug!("Reduce local file {} to find first unique entry.", tmp_file);

            let mut file = fs::File::open(tmp_file).expect("Failed to open temporary file.");

            match self.reduce_local_unique(&mut file) {
                Some(entry) => entries.push(entry),
                None => (),
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

        result.map(|text| text.trim_end_matches(',').to_string())
    }

    fn reduce_local_unique<R: Read>(&self, reader: &mut R) -> Option<entry::Entry> {
        let mut buf = Vec::with_capacity(1024);
        reader.read_to_end(&mut buf).unwrap();

        let merged_map =
            Block::parse_entries(&buf)
                .into_iter()
                .fold(HashMap::new(), |mut acc, entry| {
                    if let Some(old_record) = acc.get(&entry.key) {
                        let new_record = entry.record.merge(old_record);
                        acc.insert(entry.key, new_record);
                    } else {
                        acc.insert(entry.key, entry.record);
                    }
                    acc
                });

        self.find_first_entry_from_reduced_map(merged_map)
    }

    /// Find the unique (count == 1) word with minimum index from a given word count hash map.
    fn find_first_entry_from_reduced_map(
        &self,
        merged_map: HashMap<String, Record>,
    ) -> Option<Entry> {
        let (result, _) = merged_map.into_iter().fold(
            (None, u64::max_value()),
            |(origin, min_index), (key, record)| {
                if record.count == 1 && record.index < min_index {
                    let replaced_index = record.index;
                    let entry = entry::Entry::from_record(key, record);
                    (Some(entry), replaced_index)
                } else {
                    (origin, min_index)
                }
            },
        );

        result
    }
}
