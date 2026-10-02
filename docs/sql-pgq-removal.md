# SQL/PGQ removal and recovery

PostgreSQL 19 removed SQL/PGQ before release. This loader therefore pins
`groovemap-database-schema` at `9a50949b1810f3e61adae2f89b86acec2e20c4a3`, the
revision that removes the experimental property graph declaration and activation switch.
The promoted copy of `contracts/persistence/v1/compatibility.json` no longer advertises
`graph.catalog` or an SQL/PGQ activation contract.

No loader write path depended on that declaration. The retained code continues to own
ordinary relational writes to `graph.issued_on`, `graph.medium`, and
`graph.media_family`, and to store MusicBrainz relationships in
`musicbrainz.relationships`. The schema still supplies `graph.mb_relationship_type` for
the compatibility mapping. Delete reconciliation, bounded database retries, and the
Neo4j parity lane continue to exercise those relational interfaces. The downstream
consumers and known parity differences are recorded in [store parity](store-parity.md),
and the media ownership boundary is recorded in [graph media](graph-media.md).

Git history is the recovery record; no archive branch is maintained. The repository tree
before this cleanup is `1f0150d61519df4089cda1f990f5b9dbf3fdc6a9`. The original
contract promotion that introduced the SQL/PGQ declaration is
`bf7a0c96f42d5621d9792334ab1d32ff16d19f70`. To inspect or restore an individual file:

```console
git show 1f0150d61519df4089cda1f990f5b9dbf3fdc6a9:contracts/persistence/v1/compatibility.json
git restore --source=1f0150d61519df4089cda1f990f5b9dbf3fdc6a9 -- contracts/persistence/v1/compatibility.json
```

The matching schema implementation before removal is available from the database-schema
repository at `fecb0a43814a1e5047a5db423e7bb924986dedf6` (the first parent before merge
`9a50949b1810f3e61adae2f89b86acec2e20c4a3`). Its historical SQL/PGQ implementation can
be inspected without changing a worktree:

```console
git -C ../database-schema show fecb0a43814a1e5047a5db423e7bb924986dedf6:src/groovemap_schema/postgres.py
```
