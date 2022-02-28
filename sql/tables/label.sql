DROP TABLE IF EXISTS label;

CREATE TABLE label (
    id serial primary key,
	name text,
	contactinfo text,
	profile text,
    parent_label text,
    sublabels text[],
    urls text[],
    data_quality text
);