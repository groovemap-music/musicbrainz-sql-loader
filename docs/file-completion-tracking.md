# File and extraction completion

`musicbrainz-sql-loader` recognizes two control events alongside catalog records.

```mermaid
stateDiagram-v2
    [*] --> Consuming
    Consuming --> MarkedComplete: file_complete or extraction_complete
    MarkedComplete --> GracePeriod: cancellation delay is positive
    MarkedComplete --> CompleteSubscribed: cancellation disabled
    GracePeriod --> GracePeriod: another completion marker
    GracePeriod --> ConsumerCanceled: timer expires
    ConsumerCanceled --> BrokerClosed: all four streams marked and canceled
    BrokerClosed --> Consuming: durable queue work triggers recovery
```

## `file_complete`

The event marks one of `artists`, `labels`, `release-groups`, or `releases` complete. The
loader schedules that stream's consumer cancellation, records the stream in
`completed_files`, and acknowledges the event. A configurable grace period allows
deliveries already in flight to finish.

## `extraction_complete`

The producer publishes this version-level terminal event to every stream. Each delivery
reasserts completion for its receiving stream even when the process restarted after
acknowledging `file_complete`, preventing a false stuck state. Once all four streams are
marked complete and their consumers have canceled, the loader closes the active broker
connection and enters periodic queue-check mode.

The signal also drives delete-reconciliation of the two child-row tables every entity
kind writes, `musicbrainz.relationships` and `musicbrainz.external_links`. Because all four
kinds share those tables, the purge waits until every stream has delivered
`extraction_complete` for the same `started_at`, then deletes the rows whose `updated_at`
predates that boundary. It is skipped when a record was dead-lettered during the
extraction, when the producer reported no records for some entity kind, or when the delete
would remove 90% or more of a table. A failed purge requeues the signal and withdraws that
stream's completion. When the pinned schema does not provide `updated_at` on those tables,
reconciliation stays off and loading continues. Entity rows are never purged. Schema
constraints and migrations remain owned by `database-schema`.

## Recovery and monitoring

Completion state is intentionally not persisted locally. RabbitMQ is the durable source
of pending work, and PostgreSQL upserts are idempotent. The health response exposes:

- `message_counts` for successfully acknowledged records;
- `active_consumers` for streams currently subscribed;
- `completed_files` for terminal streams;
- `current_task` and `status`, including stuck-state detection.

The test suite covers duplicate completion events, restart recovery, cancellation timing,
and shutdown delivery churn without connecting to RabbitMQ or PostgreSQL.

This page describes only consumer state. The producer's durable download/processing state
and rules for emitting both markers are owned by
[`musicbrainz-ingestion`](https://github.com/groovemap-music/musicbrainz-ingestion/blob/main/docs/state-marker-system.md).
