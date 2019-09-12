# first-unique [![Build Status](https://travis-ci.com/tz70s/first-unique.svg?token=q2MTgdyCTSXkarGyJWZp&branch=master)](https://travis-ci.com/tz70s/first-unique)
Find first unique word from a large CSV file.

## Execution

```bash
# Run binary.
cargo run <csv_file>

# Tests
cargo test
```

## Problem Description

Given a large CSV file (~100GB) and a limited RAM machine (16GB), find the first unique word from the CSV file by single iteration.

Note: Eliminating I/O operations (number of reads, writes).

## The Idea

This problem is typically a **MapReduce** task.
We can map those words (and count) into uniform groups of strings (entries),
that we can ensure that we don't need to cross-handling across groups and therefore fit in limited memory.

For example, the following words:

```
aaaaa, aabcd, bdef, hello
```

can be partitioned into:

```
{aaaaa, aabcd}
{bdef}
{hello}
```

grouped by first letter.

Then each group can be potentially reduced to smaller size, and be written into disk for following writes.
The following writes keep the partition rules until all writes are done.

In the reduce phase,
we iterate each group to determine the unique word with minimum index.

To enhance parallelism,
the reducer can run in different threads (if fits in memory) and finally reduce the minimum index (from local optimal to global optimal).

The above approach of grouping by first letter is not a uniform distribution solution, but we can simply use hash and mod for grouping them uniformly.

## Implementation

There are some core abstractions of the implementation:

* `Entry`: the core data structure in (key, count, line number) format.
* `Group`: specifying number of groups and threads to be used.
* `Shuffler`: perform the shuffling phase.
* `Reducer`: perform two-phase reducing.

The purpose of `Entry` abstraction is for recording the read csv entries, intermediate storage format and reducing to find the unique word.

In the **shuffling** phase, assuming we have csv file as following,

```csv
hello, world, apple, is, juicy, apple,
```

after reading, the main thread will send those values with line number to worker threads by hashing and modular.
Then in each thread, converts those values in a `Block` struct, which is a length encoded of entry structure,
and then write those values into intermediate files in binary format via [bytes](https://docs.rs/crate/bytes/0.4.12) crate.

After shuffling, the **reducing** phase will read binary files and deserialise them into entries, then use a hash map to fold into a word count.
Finally run two comparison loop to find a minimum line number.

### Observations

#### Q: I/O Efficiency?

The benefits of this approach on I/O operations is that they are all sequential read/write operations,
with one write and one read for a single word.

The internal implementation use `BufReader`, `BufWriter` and `read_to_end` which are more efficient due to internal buffering.
Also, for this approach, there's no significant benefit using async I/O. Hence, the implementation chose a traditional blocking I/O and native threading model.

#### Q: The Efficiency of Hashing and Modular based Partition?

Originally, I thought hashing would have high costs for calculating partition,
which may be required to tweak the threading model to a more complex pipeline (some worker threads for hashing).
However, as profiling result, the synchronization cost is much higher than calculating hash (and index).

![call stack of shuffler](https://github.com/tz70s/first-unique/blob/master/images/callstack_shuffle.png)

#### Q: The Efficiency of Serialization & Deserialization?

They cost a little.

![call stack of reducer](https://github.com/tz70s/first-unique/blob/master/images/callstack_reduce.png)

The main CPU bottleneck of reducer is the memory allocation/de-allocation of objects.

#### Q: Cost of Storage Layout?

The `Block` struct consists of additional 24 bytes for 3 `u64` variables (offset, count and index).
Therefore, if keys in source file are small, the intermediate file are much greater as well as memory usage.
However, using `u64` is much scalable for large number of entries.

### Unresolve Bottleneck

Memory reduction has a bottleneck in reducing phase.

In theory, we can partition original files into small enough size and reduce them within small number of parallelism,
therefore ensure the total memory within machine can be fit.

However, in practice, there's two potential trade-off: limited file numbers and performance.

The current memory reduction bottleneck is using the dynamic resizing vector (which grows exponentially)
and additional space for bytes parsing. Shown in the following image.

The memory allocation graph shows that there are some _peaks_ of memory allocations (but we call `shrink_to_fit` immediately).

First, for 1 GB file (worst-case, all unique words), 128 shards and 4 simultaneous reducers.

![](https://github.com/tz70s/first-unique/blob/master/images/file_1g_128_shard_4_reducer.png)

Second, for 1 GB file (worst-case, all unique words), 128 shards and 2 simultaneous reducers.

![](https://github.com/tz70s/first-unique/blob/master/images/file_1g_128_shard_2_reducer.png)

The cause of those allocation costs come from `Vec<u8>` and `Vec<Entry>` for parsing (persist in memory at the same time),
in function `std::io::read_to_end` and `first_unique::entry::Block::parse_entries`.

![](https://github.com/tz70s/first-unique/blob/master/images/callstack_mem.png)

### Evaluation Setup

For profiling, I used my own MacBook Pro'18 for 2.3 GHz Intel Core i5 CPU (4 core with hyper-threading), 16 GB RAM and less than 80 GB Disk space (Apple SSD).
To build a release build, run the following command.

```bash
cargo build --release
```
