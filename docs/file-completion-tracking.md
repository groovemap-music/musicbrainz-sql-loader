# File and extraction completion

`musicbrainz-sql-loader` recognizes two control events alongside catalog records.

```mermaid
stateDiagram-v2
    [*] --> Consuming
    Consuming --> GracePeriod: file_complete
    GracePeriod --> ConsumerCanceled: grace period expires
    Consuming --> Complete: extraction_complete
    GracePeriod --> Complete: extraction_complete
    Complete --> Idle: every stream complete
    Idle --> Consuming: durable queue contains new work
```

## `file_complete`

The event marks one of `artists`, `labels`, `release-groups`, or `releases` complete. The
loader schedules that stream's consumer cancellation, records the stream in
`completed_files`, and acknowledges the event. A configurable grace period allows
deliveries already in flight to finish.

## `extraction_complete`

This is the terminal event for a stream. It reasserts completion even when the process
restarted after acknowledging `file_complete`, preventing a false stuck state. Once all
four streams are complete and their queues are empty, the loader can close the active
broker connection and enter periodic queue-check mode.

Unlike SQL loaders that own stale-row purging, this service does not infer deletion from
a timestamp. It stores the current MusicBrainz records supplied by the producer. Schema
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
