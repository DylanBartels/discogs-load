-- Primary keys
ALTER TABLE release ADD CONSTRAINT pkey_release PRIMARY KEY (id);
-- ALTER TABLE release_video ADD CONSTRAINT pkey_release_video PRIMARY KEY (release_id);
-- ALTER TABLE release_label ADD CONSTRAINT pkey_release_label PRIMARY KEY (release_id);

-- Indexes
CREATE INDEX idx_release on release(id);
CREATE INDEX idx_release_video on release_video(release_id);
CREATE INDEX idx_release_label on release_label(release_id);