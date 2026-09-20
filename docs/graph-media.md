# Graph media: the MusicBrainz half of `graph.issued_on`

This loader writes three relations in the `graph` schema that
[`database-schema`](https://github.com/groovemap-music/database-schema) declares and that
`discogs-sql-loader` also writes: the `graph.issued_on` edge and the `graph.medium` and
`graph.media_family` vertices. The persistence compatibility contract records all three with
both loaders as their owner. This page states the rules that makes safe.

## Where the rows come from

Every release message already carries, or has derived for it, the canonical media block ADR
0007 defines. That block is stored on `musicbrainz.releases.media`, and the same block is
projected into one `graph.issued_on` row per canonical medium. Two format entries that resolve
to the same medium — a 2xLP the provider states as two entries — are one row whose quantity is
their sum. A quantity that is absent, not a whole number, or not at least one counts as one
unit, and a quantity beyond nine digits defaults rather than overflowing the `bigint` column.

The derivation mirrors `media_edge_rows`, `_medium_label`, and `reconcile_release_media` in
`brainzgraphinator/_projections.py`, the MusicBrainz graph enricher, because both project the
same block onto the same graph: a disagreement between them is a media parity failure rather
than an error either side reports. It also matches what `groovemap_schema.postgres` projects
out of the stored block — `_MEDIA_SOURCE`'s `musicbrainz` branch and the `issued_on` body that
groups it — so the dual write and `graph.bootstrap_fill()` agree by construction.

## The release key

The graph keys a release on its **Discogs** id: `graph.release.release_id` is
`public.releases.data_id` as text, and the schema's own projection reaches the MusicBrainz side
through `musicbrainz.releases.discogs_release_id`. A MusicBrainz release that names no
`discogs_release_id` therefore has no key to write a row under, and writes none. This is what
`enrich_release` does with the same record: it counts it under
`entities_skipped_no_discogs_match` and reconciles no media, because its
`MATCH (r:Release {id: $discogs_id})` has nothing to bind either. The release document itself is
still written both times; only the graph rows wait. They appear the next time the release is
processed with a Discogs id attached, and `graph.bootstrap_fill()` covers an environment that
wants them before then.

The row is written whether or not `public.releases` already holds that Discogs release. No edge
table in the `graph` schema declares a foreign key, and the schema says why: a loader writes an
edge in the same transaction as the document it came from and may legitimately name an entity it
has not ingested yet, exactly as the enricher merges a target node as it writes the edge.

## Why the other loader's rows are safe

`graph.issued_on` is keyed on `(release_id, medium_id, source)`. The `source` column is in the
key precisely so each loader's prune reaches only its own rows. This loader writes
`source = 'musicbrainz'` as a delete-then-insert whose `DELETE` names both the release and the
source, so a row `discogs-sql-loader` wrote under `source = 'discogs'` is never in range. A
release whose media block no longer names a medium prunes to nothing rather than keeping a stale
row.

`graph.medium` and `graph.media_family` are one vocabulary shared across catalogs, so both go in
with `ON CONFLICT DO NOTHING`. That mirrors the enricher's
`MERGE (m:Medium {id: item.medium}) ON CREATE SET m.family = ..., m.label = ...`: the pass that
creates a medium is the only one that sets its family and label, and neither loader overwrites
what the other wrote.

## Transaction

All of it runs on the message's own connection, inside the transaction the delivery handler
opens for that message. The edges commit with the release document or roll back with it; the
graph never describes media the catalog did not store.
