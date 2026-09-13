# musicbrainz-sql-loader documentation

- [Configuration reference](configuration.md)
- [MusicBrainz import and restart behavior](musicbrainz-sync.md)
- [Consumer cancellation and draining](consumer-cancellation.md)
- [File and extraction completion](file-completion-tracking.md)
- [Database resilience](database-resilience.md)
- [PostgreSQL connection-budget analysis](postgres-pool-exhaustion-analysis.md)
- [Source-history provenance](extraction.md)
- [Release compliance](release-compliance.md)

The repository [README](../README.md) is the starting point for functionality,
development, validation, image naming, and compatibility identifiers.

Authoritative shared boundaries:

- [`musicbrainz-ingestion`](https://github.com/groovemap-music/musicbrainz-ingestion) owns
  MusicBrainz dump acquisition, event publication, producer state, and the catalog-event
  contract.
- [`database-schema`](https://github.com/groovemap-music/database-schema) owns PostgreSQL
  table/index definitions, compatibility policy, migrations, and initialization.
- [`python-libraries`](https://github.com/groovemap-music/python-libraries/blob/main/docs/runtime.md)
  owns shared connection resilience and telemetry behavior.
- [`deployment`](https://github.com/groovemap-music/deployment) owns Compose wiring,
  environment values, secret mounts, and fleet connection budgets.
