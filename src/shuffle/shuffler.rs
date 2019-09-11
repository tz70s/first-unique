//! Internal implementation of shuffler.

use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::mpsc;
use std::thread;

use failure::Error;
use log;

use crate::entry;
use crate::shuffle;

type Sender = mpsc::Sender<(String, usize, usize)>;
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

                        let thread_num = index % self.group.threads() as usize;

                        log::trace!(
                            "Send value {} to thread {} with index {}",
                            value,
                            thread_num,
                            index
                        );

                        senders[thread_num].send((value, lineno, index)).unwrap();
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
        (0..self.group.threads())
            .into_iter()
            .map(|thread_index| {
                let (tx, rx) = mpsc::channel();

                let group_size = self.group.size();
                let thread_nums = self.group.threads();

                let handle = thread::spawn(move || {
                    let range = group_size / thread_nums;

                    let files = (0..range)
                        .into_iter()
                        .map(|idx| {
                            let index = thread_index + idx * thread_nums;
                            let tmp_file = shuffle::temp_file(index);

                            log::debug!(
                                "Try to create temp file {} in entry format for future reducing.",
                                tmp_file
                            );
                            fs::File::create(tmp_file).expect("Can't create temp file")
                        })
                        .collect();

                    entry_writer(files, thread_nums, rx);
                });

                (tx, handle)
            })
            .unzip()
    }
}

fn entry_writer<W: Write>(
    target: Vec<W>,
    thread_nums: u32,
    rx: mpsc::Receiver<(String, usize, usize)>,
) {
    assert!(thread_nums > 0);

    let mut writers: Vec<_> = target.into_iter().map(BufWriter::new).collect();

    for (key, lineno, file_index) in rx {
        let block = entry::Block::create(key, lineno);
        let index = file_index / thread_nums as usize;

        // We'll panic here and the parent (main) thread can be notified and safely panic as well.
        writers[index]
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
                tx.send((val.to_string(), idx, 0)).unwrap();
            }
        });

        let mut entries = vec![];
        entry_writer(vec![&mut entries], 1, rx);

        let entries = Block::parse_entries(entries);

        let expect: Vec<_> = source
            .into_iter()
            .enumerate()
            .map(|(index, text)| Entry::new(text.to_string(), index))
            .collect();

        assert_eq!(entries, expect);
    }
}
