# History-preserving extraction

The source was migration branch `wt/bead/issue/discogsography-2kpm.17` at
`69d90758` in the unchanged monorepo. A disposable clone retained
`brainztableinator/`, `tests/brainztableinator/`, the applicable MusicBrainz,
PostgreSQL, and resilience documents, and `LICENSE`; the owned tests were promoted to
`tests/`.

The exact `git filter-repo` arguments were:

```text
--path brainztableinator/
--path tests/brainztableinator/
--path LICENSE
--path docs/consumer-cancellation.md
--path docs/database-resilience.md
--path docs/file-completion-tracking.md
--path docs/musicbrainz-sync.md
--path docs/postgres-pool-exhaustion-analysis.md
--path-rename tests/brainztableinator/:tests/
```

The filter retained 73 relevant commits and no tags. The current tree is MIT licensed by
owner decision; earlier license revisions remain in history. The original monorepo and its
refs were not rewritten or deleted.
