# Discogs-load

[![ci-workflow](https://github.com/dylanbartels/discogs-load/actions/workflows/ci.yml/badge.svg)](https://github.com/dylanbartels/discogs-load/actions?query=workflow%3ACI+branch%3Amaster)

A Rust application that inserts [Discogs data dumps](http://www.discogs.com/data/) into Postgres.

Discogs-load uses a simple [state machine](https://en.wikipedia.org/wiki/Finite-state_machine) with the quick-xml Rust library to parse the monthly data dump of discogs and load it into postgres. At moment of writing the largest file of the monthly dump is ~10 gb compressed and takes ~15 minutes to parse and load on a Mac air m1.

Inspired by [discogs-xml2db](https://github.com/philipmat/discogs-xml2db) and [discogs2pg](https://github.com/alvare/discogs2pg).

## Local binary installation

Compile your own binary, which requires Rust.

```
cargo build --bin discogs-load --release
./target/release/discogs-load --help
```

Or download a compressed binary compiled by the Github actions from the [Releases](https://github.com/dylanbartels/discogs-load/releases) page for different platforms and architectures.

The binary needs to be made executable after downloading:

```bash
$ gunzip discogs-load-aarch64-apple-darwin.gz
$ chmod +x discogs-load-aarch64-apple-darwin
$ ./discogs-load-aarch64-apple-darwin --help
discogs-load 0.1.1

USAGE:
    discogs-load [OPTIONS] [FILE(S)]...

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --batch-size <batch-size>      Number of rows per insert [default: 10000]
        --db-host <db-host>            Database host [default: localhost]
        --db-name <db-name>            Database name [default: discogs]
        --db-password <db-password>    Database password [default: dev_pass]
        --db-user <db-user>            Database user [default: dev]

ARGS:
    <FILE(S)>...    Path to one or more discogs monthly data dump files, still compressed
```

## Usage

Download the releases data dump [here](http://www.discogs.com/data/), and run the binary with the path to the gz compressed file as only argument. For the example below we'll use a dockerized postgres instance.

```
docker-compose up -d postgres
./discogs-load-aarch64-apple-darwin discogs_20211201_releases.xml.gz discogs_20220201_labels.xml.gz
```

## Tests

If you don't want to run the huge releases file, it is possible to run a smaller example file like so:

```
docker-compose up -d postgres
cargo run --bin discogs-load discogs-load/test_data/releases.xml.gz discogs-load/test_data/artists.xml.gz
```

And do a small manual test:

```
docker exec -it discogs-load-postgres-1 /bin/bash
psql -U dev discogs
select * from release;
```

## Contributing/Remaining todo

- Create a parser for the masters dataset
- Create a proper relational database schema