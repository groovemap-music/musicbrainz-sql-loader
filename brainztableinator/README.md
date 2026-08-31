# Python compatibility package

The `brainztableinator` directory contains the implementation of GrooveMap's
`musicbrainz-sql-loader` service. Its directory and import name are retained for Python
compatibility; the supported executable and runtime identity are
`musicbrainz-sql-loader`.

```mermaid
flowchart LR
    CLI[musicbrainz-sql-loader executable] --> Package[brainztableinator Python package]
    Package --> Service[MusicBrainz SQL loading service]
```

For service behavior, configuration, development, and operations, use the repository
[README](../README.md) and [documentation index](../docs/README.md).
