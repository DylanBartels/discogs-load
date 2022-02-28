DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
    id serial primary key,
    name text,
    real_name text,
    profile text,
    data_quality text,
    name_variations text[],
    urls text[],
    aliases text[],
    members text[]
);
