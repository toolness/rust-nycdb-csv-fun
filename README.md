## Motivation

Updating [NYC-DB][] takes a long time because each massively large
table must be rebuilt from scratch using source CSV files. This
results in a lot of duplicate effort (and expensive CPU cycles)
because most of the data already existed in the database.

This experiment explores tracking changes across different versions
of multi-gigabyte CSV files. While it doesn't currently offer
a concrete solution to the updating problem, it could be used as
a jumping-off point for implementing a quick and low-cost update
mechanism to NYC-DB.

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

Note that `log.csv` contains all the rows that have ever existed across
all revisions, while `log.revisions.csv` is essentially an index into
that file by revision number.

[NYC-DB]: https://github.com/aepyornis/nyc-db/
[Rust]: https://www.rust-lang.org/
