<!-- User-visible changes since the last release, drafted by each pull request. See packaging/RELEASING.md. -->

## New

- **Two-way sync with [Feedbin](https://feedbin.com).** `omafeed feedbin login --email
  you@example.com` saves the account after checking it, and `omafeed sync` sends this library's
  read, star, subscribe and unsubscribe changes before taking Feedbin's subscriptions, tags,
  entries, unread and starred state. With an account set up, `omafeed refresh` and the window's
  scheduled refresh run that sync instead of fetching feeds, so nothing is fetched twice, and a
  read or star change made here and not yet sent wins over the account's own value. Feedbin's
  tags are flat names, so a folder is written `Work/Blogs` and a feed has at most one. A library
  new to an account reads the newest five pages of entries; pass `omafeed sync --pages N` once
  for a longer backlog, after which only what is new is read. Anything older stays on Feedbin.
- `omafeed sync [--pages N] [--json]`, `omafeed feedbin status`, and `omafeed refresh --json`
  printing the sync's own summary while an account is set up.

## Upgrade notes

- **The library moves to schema version 5,** which stores Feedbin's IDs and the bookkeeping a
  two-way sync needs. It is upgraded the first time this version opens it. Older versions then
  refuse it with "Database was created by a newer Omafeed version", so back up the data
  directory (`~/.local/share/omafeed`) first if you may need to go back.

## Requirements

- **Building from source needs Rust 1.89 or newer** (was 1.88), because rusqlite 0.41, which omafeed uses to keep your library in SQLite, requires it ([#48](https://github.com/pch/omafeed/pull/48)). The packages and archives are unaffected.
