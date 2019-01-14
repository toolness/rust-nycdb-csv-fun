## Motivation

Updating [NYC-DB][] takes several hours because each massively large
table must be rebuilt from scratch using source CSV files. This
results in a lot of duplicate effort (and expensive CPU cycles)
because most of the data already existed in the database.

This experiment explores tracking changes across different versions
of multi-gigabyte CSV files. While it doesn't currently offer
a concrete solution to the updating problem, it could be used as
a jumping-off point for implementing a quick and low-cost update
mechanism to NYC-DB.

An additional motivation is the simple fact that it's been about
a year since I wrote anything in Rust. I'd heard [stories][]
about how much faster Rust was at parsing plain-text files than
languages like Ruby or Python, so I thought this project might
be a nice fit, and a good excuse to play with Rust again.

[stories]: https://andre.arko.net/2018/10/25/parsing-logs-230x-faster-with-rust/

## Quick start

You will need [Rust][].

Install the tool:

```
cargo install --path .
```

Then run:

```
nycsv add hpd_violations.csv
```

This will create revision 1 of the data.

Now make a minor edit to `hpd_violations.csv` and re-run the above command.
It will create revision 2 of the data, reporting the number of rows that
were added and/or updated.

You can now export the changed data with:

```
nycsv export 2
```

## How it works

The prototype assumes that each CSV has rows that can be uniquely
identified by a single numeric primary key found in the first
column. It also assumes that rows are never deleted from one
revision to the next, but that they can be added or updated.

The algorithm is fairly simple: it constructs a
hash map from primary keys to [BLAKE2][] hashes of the
contents of a row, and uses it to determine what rows in
a CSV have changed from one revision to the next. While the
entire hash map must remain resident in memory at all times,
CSV data is streamed.

The tool also maintains the following files:

* `log.csv` contains all the rows that have ever existed across
  all revisions. This means that it contains duplicate rows for
  anything that changed between revisions.

* `log.revisions.csv` is essentially an index into `log.csv`
  by revision number, recording the byte offset each revision
  starts at, and how many rows it consists of.

* `log.cache.dat` is a serialization of the hash map. It can
  actually be deleted: if it's not found, the entire log will
  be replayed to rebuild it.

## Performance

The following was run on an Intel i5-7600K running at 3.80 Mhz with a
solid state drive.

Adding an initial 2.3 GB CSV takes about 23 seconds:

```
$ nycsv add hpd_violations-2019-01-11.csv
Processing log.csv...
472 B / 472 B [=========================================] 100.00 % 17.18 MB/s
Processing hpd_violations-2019-01-11.csv...
2.23 GB / 2.23 GB [====================================] 100.00 % 102.49 MB/s
Finished processing 5,104,859 records with 5,104,859 additions and 0 updates.
Saving log cache with 5,104,859 entries...
[==================================================================] 100.00 %
Wrote revision 1.
Finished in 23 seconds.
```

This initial revision takes longer than subsequent ones because it needs
to write every single record it sees to the log file. Subsequent
revisions will take less time, as most rows will have identical hashes.

Adding a subsequent CSV that was published on the following day takes
about 17 seconds:

```
$ nycsv add hpd_violations-2019-01-12.csv
Loading log cache with 5,104,859 entries...
[==================================================================] 100.00 %
Processing hpd_violations-2019-01-12.csv...
2.23 GB / 2.23 GB [====================================] 100.00 % 156.57 MB/s
Finished processing 5,106,615 records with 1,756 additions and 8,827 updates.
Saving log cache with 5,106,615 entries...
[==================================================================] 100.00 %
Wrote revision 2.
Finished in 17 seconds.
```

Running `nycsv export 2` yields a CSV that is less than 5 megabytes in size,
which is much easier to process than the original file.

[NYC-DB]: https://github.com/aepyornis/nyc-db/
[Rust]: https://www.rust-lang.org/
[BLAKE2]: https://blake2.net/
