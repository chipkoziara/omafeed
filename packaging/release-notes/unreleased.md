<!-- User-visible changes since the last release, drafted by each pull request. See packaging/RELEASING.md. -->

## Improved

- **The article list and the sidebar stay where they are scrolled** when a refresh or another program reloads them. Before, both jumped back to the top, so a refresh that landed while you were reading the list moved it out from under you.

## Requirements

- **Building from source needs Rust 1.89 or newer** (was 1.88), because rusqlite 0.41, which omafeed uses to keep your library in SQLite, requires it ([#48](https://github.com/pch/omafeed/pull/48)). The packages and archives are unaffected.
