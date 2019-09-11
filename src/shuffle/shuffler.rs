//! Internal implementation of shuffler.

use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::mpsc;
use std::thread;

use failure::Error;
use log;

use crate::entry;
use crate::shuffle;

type Sender = mpsc::Sender<(String, usize)>;
type Join = thread::JoinHandle<()>;

pub struct Shuffler {
    group: shuffle::Group,
}

impl Shuffler {
    /// Create a new shuffler.
    pub fn new(group: shuffle::Group) -> Shuffler {
        Shuffler { group }
    }

    pub fn run_partition<R: Read>(&self, csv_source: R) -> Result<(), Error> {
        let buff_reader = BufReader::new(csv_source);

        let (senders, joins) = self.spawn_mappers();
        {
            // To avoid deadlock, we need to drop senders here to terminate mapper threads.
            let senders = senders;

            for (lineno, line) in buff_reader.lines().enumerate() {
                match line {
                    Ok(value) => {
                        // Note: the value here contains a ',' at the end.
                        // We don't need to do early trim to reduce overhead.
                        // But still need to reduce it while finding the target value.
                        let index = self.group.make_index(&value) as usize;

                        log::trace!("Send value {} to thread index {}", value, index);

                        senders[index].send((value, lineno)).unwrap();
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

        for join in joins {
            // The error return from the join indicates that any of child thread panic.
            // Then we can panic here as well, since it's unexpected programmatically error.
            join.join().unwrap();
        }

        Ok(())
    }

    fn spawn_mappers(&self) -> (Vec<Sender>, Vec<Join>) {
        (0..self.group.size())
            .into_iter()
            .map(|idx| {
                let (tx, rx) = mpsc::channel();

                let tmp_file = shuffle::temp_file(idx);

                log::debug!(
                    "Try to create temp file {} in entry format for future reducing.",
                    tmp_file
                );

                let file = fs::File::create(tmp_file).expect("Can't create temp file");

                let handle = thread::spawn(move || entry_writer(file, rx));

                (tx, handle)
            })
            .unzip()
    }
}

fn entry_writer<W: Write>(target: W, rx: mpsc::Receiver<(String, usize)>) {
    let mut writer = BufWriter::new(target);

    for (key, index) in rx {
        let block = entry::Block::create(key, index);

        // We'll panic here and the parent (main) thread can be notified and safely panic as well.
        writer
            .write(&block.as_bytes())
            .expect("Failed to write block within entry writer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{Block, Entry};

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

        let entries = Block::parse_entries(&entries);

        let expect: Vec<_> = source
            .into_iter()
            .enumerate()
            .map(|(index, text)| Entry::new(text.to_string(), index))
            .collect();

        assert_eq!(entries, expect);
    }
}
