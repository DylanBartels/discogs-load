FROM rust:latest as build

WORKDIR /app

COPY Cargo.lock Cargo.toml ./
COPY src src
COPY sql sql
COPY tests tests

RUN cargo build --release --all

FROM debian:buster-slim

WORKDIR /app
COPY --from=build /app/target/release/discogs-load ./

ENTRYPOINT ["/app/discogs-load"]
CMD ["/app/tests/data/discogs_test_releases.xml.gz"]