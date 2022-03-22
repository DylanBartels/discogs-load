DROP TABLE IF EXISTS label;

CREATE TABLE label (
    id int not null,
	name text,
	contactinfo text,
	profile text,
    parent_label text,
    sublabels text[],
    urls text[],
    data_quality text
);