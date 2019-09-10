//! Internal implementation of shuffler.

use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::sync::mpsc;
use std::thread;

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

    pub fn run_partition<R: Read>(&self, csv_source: R) -> Result<(), io::Error> {
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
                    Err(err) => return Err(err),
                }
            }
        }

        for join in joins {
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
                    "Create temp file {} in entry format for future reducing.",
                    tmp_file
                );

                let file = fs::File::create(tmp_file).expect("Failed to create temporary file.");

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

        writer
            .write(&block.as_bytes())
            .expect("Unexpected write failure");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
