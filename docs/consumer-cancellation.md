# Consumer cancellation and draining

Each MusicBrainz stream has its own RabbitMQ consumer. Cancellation releases broker
resources after a stream finishes without interrupting in-flight deliveries.

```mermaid
sequenceDiagram
    participant Producer as musicbrainz-ingestion
    participant Broker as RabbitMQ
    participant Loader as musicbrainz-sql-loader
    participant Timer as Grace-period timer

    Producer->>Broker: file_complete or extraction_complete
    Broker->>Loader: deliver terminal marker
    Loader->>Timer: schedule cancellation
    Loader->>Broker: acknowledge marker
    Timer-->>Loader: grace period expires
    Loader->>Broker: cancel stream consumer
    Note over Loader,Broker: Other stream consumers remain active
```

`CONSUMER_CANCEL_DELAY` controls the grace period and defaults to 300 seconds. Set it to
`0` to leave consumers subscribed. If another completion marker arrives before the timer
expires, the loader cancels the existing task and starts one new grace-period timer for that
stream.


Cancellation always waits for broker `cancel-ok` (`nowait=False`), bounded by a
five-second broker RPC deadline and a five-second task deadline. An unconfirmed
cancel keeps its active tag and marks health unhealthy; the periodic checker
requests recovery even though that tag would otherwise block idle detection.
Recovery closes the delivery channel/connection with bounded waits, retaining
handles and registry evidence if closure cannot be confirmed. It resubscribes
only after closure is confirmed. Registered tags remain available for close logs
after normal per-stream cancellation, preserving never-started/stalled stream
instrumentation.

A new record clears its type's old completion marker and cancels the previous
extraction's grace timer, re-arming starvation detection. Old timer cleanup removes
only its own reference, protecting any replacement timer. Interrupting an already
started cancellation RPC also requests recovery because broker state is uncertain.

## Graceful process shutdown

Process shutdown is a separate path from file completion:

1. Cancel and join pending grace timers, then confirm consumer cancellations.
   After the first failed RPC, close the uncertain channel before further teardown.
2. Cancel progress, queue-check, and pending cancellation tasks.
3. Close the RabbitMQ connection, which requeues any unsettled deliveries once.
4. Close the PostgreSQL pool and health server.

A delivery that reaches the handler after shutdown begins is deliberately left unsettled; a
handler already inside its transaction may still finish normally. An immediate
`nack(requeue=True)` while a subscription remains active would redeliver the same message in
a tight loop and consume the quorum queue's delivery budget.

Regression coverage for this ordering lives in
[`tests/test_consumer_cancellation.py`](../tests/test_consumer_cancellation.py),
[`tests/test_shutdown_delivery_churn.py`](../tests/test_shutdown_delivery_churn.py) and
the drain tests in [`tests/test_brainztableinator.py`](../tests/test_brainztableinator.py).
The producer's authoritative completion semantics are documented in the
[`musicbrainz-ingestion` state-marker system](https://github.com/groovemap-music/musicbrainz-ingestion/blob/main/docs/state-marker-system.md#completion-signals).
