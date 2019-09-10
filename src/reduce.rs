//! Reducer

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read};

use log;

use crate::entry;
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

            let file = fs::File::open(tmp_file).expect("Failed to open temporary file.");

            match self.reduce_local_unique(file) {
                Some(entry) => entries.push(entry),
                None => (),
            }
        }

        let mut result = None;
        let mut min_lineno = u64::max_value();

        for entry in entries {
            if entry.record.index < min_lineno {
                min_lineno = entry.record.index;
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
            if record.count == 1 && record.index < min_lineno {
                min_lineno = record.index;
                result = Some(entry::Entry::from_record(val, record));
            }
        }

        result
    }
}
