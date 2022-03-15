DROP TABLE IF EXISTS artist;

CREATE TABLE artist (
    id int not null,
    name text,
    real_name text,
    profile text,
    data_quality text,
    name_variations text[],
    urls text[],
    aliases text[],
    members text[]
);
