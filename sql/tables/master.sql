DROP TABLE IF EXISTS master CASCADE;
DROP TABLE IF EXISTS master_artist CASCADE;

CREATE TABLE master (
    id integer NOT NULL,
    title text,
    release_id integer NOT NULL,
    year integer,
    notes text,
    genres text[],
    styles text[],
    data_quality text
 );

 CREATE TABLE master_artist (
    artist_id integer NOT NULL,
    master_id integer NOT NULL,
    name text,
    anv text,
    role text
);