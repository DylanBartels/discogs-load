FROM rust:latest as build

WORKDIR /app

COPY Cargo.lock Cargo.toml ./
COPY discogs-load discogs-load
COPY xtask xtask
COPY sql sql

RUN cargo build --bin discogs-load --release

FROM debian:buster-slim

WORKDIR /app
COPY --from=build /app/target/release/discogs-load ./
COPY --from=build /app/sql ./sql
COPY --from=build /app/discogs-load/tests ./tests

CMD /app/discogs-load --db-host ${POSTGRES_HOST} /app/tests/data/discogs_test_releases.xml.gz