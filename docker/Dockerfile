FROM rust:latest as build

WORKDIR /app

COPY Cargo.toml ./
COPY discogs-load discogs-load
COPY xtask xtask
COPY sql sql

RUN cargo build --bin discogs-load --release

FROM debian:buster-slim

WORKDIR /app
COPY --from=build /app/target/release/discogs-load ./
COPY --from=build /app/sql ./sql
COPY --from=build /app/discogs-load/tests ./tests

CMD /app/discogs-load --db-host ${POSTGRES_HOST}  /app/test_data/releases.xml.gz /app/test_data/labels.xml.gz /app/test_data/artists.xml.gz