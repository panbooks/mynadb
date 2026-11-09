# Release preparation

This is a local preparation checklist, not a new release announcement.

Current observed release: GitHub `v0.0.1`, published 2026-11-09, no binary assets.
Cargo declares 0.1.0; historical local tags include v0.1.0 and v0.1.1. Agree on
the version scheme and check the remote tags before publishing another release.

- [ ] Confirm ownership and root license; complete third-party attribution review.
- [ ] Choose the exact source SHA and version; ensure Cargo and tag policy agree.
- [ ] Name supported platforms; do not imply platforms that were not tested.
- [ ] Record OS, architecture, Rust/Cargo/protoc and native build prerequisites.
- [ ] Build with the committed lockfile and run the selected core tests.
- [ ] Run local write/query/flush/restart with the release binary.
- [ ] Test documented installation from a fresh directory.
- [ ] Write release notes: changes, setup, compatibility, durability settings,
      known issues, and exact validation coverage.
- [ ] If shipping binaries, record source SHA, target triple and SHA-256 checksums.
- [ ] Download the published assets and verify checksums and startup independently.

## Release-note outline

```text
Version / source SHA:
Supported and tested platforms:
Implemented changes:
Commands and tests actually run:
Installation:
WAL and synchronization settings used in tests:
Compatibility changes:
Known issues and unverified features:
Binary checksums (if binaries are provided):
Feedback: https://github.com/panbooks/mynadb/issues
```

Do not create another version tag merely to complete the promotion checklist.
