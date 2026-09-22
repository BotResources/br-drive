# Changelog

All notable changes to `br-drive` are documented here. The whole workspace
ships **one version**: every crate inherits `version.workspace = true`, and a
single git tag `v{version}` releases the set. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## Unreleased

### Added

- Repository skeleton on the `br-service-engine` template, pinned on engine
  `v0.3.0`: the `br-drive` library crate with `br_drive::migrations()` (schema
  `drive`, band `9_121_000_001..=9_121_999_999`) and the roster-shaped
  `drive_slice!` macro (`prefix = …; principal = …`), the `DriveHost` bound the
  host principal implements, and the `br-drive-example` host — a fictional
  `workspace` service that embeds the slice through `compose_service!` and
  `BootPlan.libraries` — with its in-repo e2e harness (real PostgreSQL, NATS
  JetStream, `graphql-transport-ws`) and one smoke scenario.
