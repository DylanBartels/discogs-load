-- Primary keys
ALTER TABLE release ADD CONSTRAINT pkey_release PRIMARY KEY (id);
-- ALTER TABLE release_video ADD CONSTRAINT pkey_release_video PRIMARY KEY (release_id);
-- ALTER TABLE release_label ADD CONSTRAINT pkey_release_label PRIMARY KEY (release_id);

-- Indexes
CREATE INDEX idx_label on label(id);

CREATE INDEX idx_artist on artist(id);

CREATE INDEX idx_release on release(id);
CREATE INDEX idx_release_video on release_video(release_id);
CREATE INDEX idx_release_label on release_label(release_id);

CREATE INDEX idx_master_artist_master on master_artist(master_id);
CREATE INDEX idx_master_artist_artist on master_artist(artist_id);