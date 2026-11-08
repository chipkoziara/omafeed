-- A library as Omafeed left it at schema version 5, frozen: never edit this file.
-- Feedbin's IDs and sync bookkeeping, as this version's migration adds them.
INSERT INTO folders(id, parent, name) VALUES(1, NULL, 'Work'), (2, 1, 'Sub');
INSERT INTO feeds(id, folder, title, url, site_url, etag, modified, last_attempt, last_success,
                  next_fetch, failures, error)
VALUES(1, 2, 'Local Feed', 'https://example.org/feed.xml', 'https://example.org/', '"v1"',
       'Mon, 01 Jan 2024 00:00:00 GMT', 1704067200, 1704067200, 1704069000, 0, NULL),
      (2, NULL, 'Broken', 'https://example.net/rss', '', NULL, NULL, 1704067200, NULL,
       1704067440, 3, 'HTTP status server error (503 Service Unavailable)');
INSERT INTO articles(id, feed_id, identity, title, url, author, published, first_seen, html, text)
VALUES(1, 1, 'one', 'Links', 'https://example.org/1', 'Ann', 1704067200, 1704067200,
       '<p>On <a href="https://x.org">Spotify</a>, Apple</p>', 'On Spotify, Apple'),
      (2, 1, 'two', 'Details', 'https://example.org/2', '', 1704153600, 1704153600,
       '<details><summary>Changes</summary>Fixed</details>', 'Changes Fixed'),
      (3, 2, 'three', 'Plain', '', '', 1704240000, 1704240000, '<p>Plain words</p>', 'Plain words');
UPDATE article_state SET read = 1, starred = 1 WHERE article_id = 1;
UPDATE article_state SET read = 1 WHERE article_id = 3;


ALTER TABLE feeds ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
UPDATE feeds SET revision = id;
CREATE TABLE meta(
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next_revision INTEGER NOT NULL
);
INSERT INTO meta(id, next_revision) VALUES(1, (SELECT COALESCE(max(id), 0) + 1 FROM feeds));

ALTER TABLE feeds ADD COLUMN feedbin_id INTEGER;
ALTER TABLE feeds ADD COLUMN feedbin_tagging INTEGER;
ALTER TABLE feeds ADD COLUMN feedbin_subscription INTEGER;
ALTER TABLE feeds ADD COLUMN feedbin_title TEXT;
ALTER TABLE feeds ADD COLUMN feedbin_folder TEXT;
ALTER TABLE articles ADD COLUMN feedbin_id INTEGER;
ALTER TABLE article_state ADD COLUMN read_pending INTEGER NOT NULL DEFAULT 0;
ALTER TABLE article_state ADD COLUMN starred_pending INTEGER NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX feed_feedbin ON feeds(feedbin_id) WHERE feedbin_id IS NOT NULL;
CREATE UNIQUE INDEX article_feedbin ON articles(feedbin_id) WHERE feedbin_id IS NOT NULL;
CREATE TABLE feedbin_deleted(
    kind TEXT NOT NULL,
    remote_id INTEGER,
    name TEXT,
    UNIQUE(kind, remote_id, name)
);
CREATE TABLE feedbin_meta(
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cursor TEXT
);
INSERT INTO feedbin_meta(id) VALUES(1);
PRAGMA user_version = 5;
