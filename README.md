# Discogs-load

A Rust application that inserts [Discogs data dumps](http://www.discogs.com/data/) into Postgres.

Discogs-load uses a simple [state machine](https://en.wikipedia.org/wiki/Finite-state_machine) with the quick-xml Rust library to parse the monthly data dump of discogs and load it into postgres. At moment of writing the largest file of the monthly dump is ~10 gb compressed and takes ~20 minutes to parse and load on a mac air m1.

Inspired by [discogs-xml2db](https://github.com/philipmat/discogs-xml2db) and [discogs2pg](https://github.com/alvare/discogs2pg).

# Installation

Create a binary.

```
cargo build --release
```

## Usage

Download the releases data dump [here](http://www.discogs.com/data/), and run the binary with the path to the gz compressed file as only argument.

```
docker-compose up -d postgres
./target/release/discogs-load discogs_20211201_releases.xml.gz
```

## Tests

If you don't want to run the huge releases file, it is possible to run a smaller example file like so:

```
docker-compose up -d postgres
cargo run tests/data/discogs_test_releases.xml.gz
```

And do a small manual test:

```
docker exec -it discogs-load_postgres_1 /bin/bash
psql -U dev discogs
select * from release;
```

## Contributing/Remaining todo

- Implement [COPY_IN](https://docs.rs/postgres/0.15.2/postgres/stmt/struct.Statement.html#method.copy_in)
    - Postgres COPY is faster than the current multi row insertion.
    - will also refactor current ugly functions of `write_table`
- Other (smaller) files from the monthly discogs data dump
    - labels
    - artists
    - masters