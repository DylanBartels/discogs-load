DROP TABLE IF EXISTS release;
DROP TABLE IF EXISTS release_label;
DROP TABLE IF EXISTS release_video;

CREATE TABLE release (
    id int NOT NULL,
    status text,
    title text,
    country text,
    released text,
    notes text,
    genres text[],
    styles text[],
    master_id int,
    data_quality text
);

CREATE TABLE release_label (
    id serial,
    release_id int NOT NULL,
    label_id int,
    label text,
    catno text
);

CREATE TABLE release_video (
    id serial,
    release_id int NOT NULL,
    duration int,
    src text,
    title text
);