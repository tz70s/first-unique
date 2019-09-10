# first-unique [![Build Status](https://travis-ci.com/tz70s/first-unique.svg?token=q2MTgdyCTSXkarGyJWZp&branch=master)](https://travis-ci.com/tz70s/first-unique)
Find first unique word from a large CSV file.

## Execution

```bash
# From sample data.
cargo run --example check

# Integration tests (mostly are ignored due to generate large file size).
cargo test -- --ignored
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

by first letter.

Then each group can be potentially reduced to smaller size, and be written into disk for following writes.
The following writes keep the partition rules until all writes are done.

In the reduce phase,
we iterate each group to determine the unique word with minimum index.

To enhance parallelism,
the reducer can run in different threads (if fits in memory) and finally reduce the minimum index (from local optimal to global optimal).

The basic idea is not a uniform distribution solution, but we can simply use hash and mod for grouping them uniformly.

## Optimization Tricks

