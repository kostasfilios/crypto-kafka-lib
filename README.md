# Crypto Kafka Library

A Kafka producer and consumer configuration library for Spring Boot applications.

## Features

- Pre-configured Kafka producer and consumer
- Spring Boot auto-configuration
- Configurable serialization
- Message sending utilities

## Usage

### Gradle

```gradle
dependencies {
    implementation 'com.github.YourUsername:crypto-kafka-lib:1.0.0'
}
```

### Maven

```xml
<dependency>
    <groupId>com.github.YourUsername</groupId>
    <artifactId>crypto-kafka-lib</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Configuration

Add to your `application.properties`:

```properties
kafka.bootstrap-servers=localhost:9092
kafka.consumer.group-id=your-group-id
```

## Components

- `KafkaMessageProducer` - Send messages to Kafka topics
- `KafkaProducerConfig` - Producer configuration
- `KafkaConsumerConfig` - Consumer configuration
- `EventPublisherChannels` / `EventPublisher` - Async, keyed event publishing per named channel (v1.3.0, below)

## Event publisher channels

Since v1.3.0 (`com.crp.system.libs.kafka.publisher`). A channel is a named, independently configured publisher with
its own Kafka producer, queue and limits. Business code never blocks on Kafka and never gets an exception from it.
Nothing changes for a service until it enables a channel; `KafkaMessageProducer` is untouched.

### Usage

```kotlin
@Component
class SelfExclusionReportingEvents(
    channels: EventPublisherChannels,
    @Value("\${reporting.events.topic.responsible-gambling}") private val topic: String,
) {
    private val reporting = channels.get("reporting")

    fun lifted(event: SelfExclusionReportingEvent) = reporting.publish(topic, event.playerId.toString(), event)
}
```

- `publish(topic, key, event, headers)` serializes the event with the channel's Gson (snake_case by default, nulls kept,
  no HTML escaping) on the caller thread, then queues it.
- `publishJson(topic, key, json, headers)` sends JSON the caller already built (topics with their own JSON style).
- `publishWithResult(...)` also returns a `CompletableFuture<PublishResult>`: it completes on the broker's
  acknowledgement, or fails with a `PublishFailedException` carrying the `PublishFailure`. It usually completes on the
  producer's I/O thread, so continuations must be quick or use `...Async(executor)`.
- `EventIds.nameBased(name)` (UUIDv5, OID namespace), `EventIds.random()` (v4) and `EventIds.utcMillis(instant)`
  (`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`) are the reporting contract's ids and timestamps.
- A channel that is disabled, unknown or misconfigured returns a publisher that does nothing (an unknown name logs one WARN).
- No argument can make a call throw: the parameters are nullable for Java callers and values of Java types. A null
  topic is an `INTERNAL` failure, a null event or json a `SERIALIZATION` failure, and a header with a null name or
  value is left out.

Service tests need no broker:

```kotlin
val channels = RecordingEventPublisherChannels()
SelfExclusionReportingEvents(channels, "account.responsible_gambling.events").lifted(event)
assertThat(channels.recorded("reporting").single().key).isEqualTo("42")
assertThat(channels.recorded("reporting").single().json).isEqualTo("""{"event_type":"self_exclusion_lifted",...}""")
```

### Properties

All keys are per channel under `crypto.kafka.publisher.channels.<name>.`; missing keys take the defaults below.

```properties
crypto.kafka.publisher.summary-interval=10s                      # failure/drop summary period
crypto.kafka.publisher.channels.reporting.enabled=${reporting.events.enabled:false}
crypto.kafka.publisher.channels.reporting.serializer=SNAKE_CASE  # or IDENTITY
crypto.kafka.publisher.channels.reporting.lanes=2                # 1-16 worker threads
crypto.kafka.publisher.channels.reporting.queue-capacity=10000   # split across the lanes; at most 1,000,000, and 250,000 per lane
crypto.kafka.publisher.channels.reporting.backpressure=DROP      # DROP | CALLER_RUNS | BLOCK_WITH_TIMEOUT
crypto.kafka.publisher.channels.reporting.block-timeout=100ms    # BLOCK_WITH_TIMEOUT only
crypto.kafka.publisher.channels.reporting.ordering=PER_KEY       # PER_KEY | NONE
crypto.kafka.publisher.channels.reporting.retry.max-attempts=1   # 1 = only the producer's own retries
crypto.kafka.publisher.channels.reporting.retry.initial-backoff=200ms   # > 0
crypto.kafka.publisher.channels.reporting.retry.multiplier=2.0
crypto.kafka.publisher.channels.reporting.retry.max-backoff=5s         # > 0 and >= initial-backoff
crypto.kafka.publisher.channels.reporting.producer.max-block-ms=2000
crypto.kafka.publisher.channels.reporting.producer.buffer-memory=8388608
crypto.kafka.publisher.channels.reporting.producer.linger-ms=5
crypto.kafka.publisher.channels.reporting.producer.request-timeout-ms=10000
crypto.kafka.publisher.channels.reporting.producer.delivery-timeout-ms=30000   # >= linger + request timeout
crypto.kafka.publisher.channels.reporting.producer.acks=all      # idempotence is on only with acks=all
crypto.kafka.publisher.channels.reporting.producer.compression-type=none
crypto.kafka.publisher.channels.reporting.producer.extra.<any.producer.key>=<value>   # applied last (never transactional.id)
crypto.kafka.publisher.channels.reporting.producer.missing-topic-cooldown=30s   # 0 turns it off; see "Missing topics" below
crypto.kafka.publisher.channels.reporting.topics=account.player.events,payments.request.events   # checked at startup, never created
crypto.kafka.publisher.channels.reporting.shutdown-timeout=5s
```

- **Producer.** Each enabled channel builds its own `KafkaProducer` from the service's one `KafkaTemplate` (its
  connection and security settings) plus the channel's limits. The client id is `<spring.application.name>-<channel>`.
  The channel never uses Kafka transactions (a `transactional.id` in `producer.extra` is removed with a WARN) and adds no
  `KafkaTemplate` or `ProducerFactory` bean. With several templates, the `@Primary` one is used.
- **Ordering.** Under `PER_KEY` the key's hash picks the lane, so one key keeps its order end to end. Two cases can still
  reorder one key: `CALLER_RUNS` under overload, and app retries (`max-attempts` > 1), which re-enter at the lane's tail.
- **App retries can duplicate.** An app retry re-sends a record whose earlier attempt may already be on the broker: after
  a delivery timeout (`Expiring … record(s)`) or `NotEnoughReplicasAfterAppendException` the write can have happened.
  The idempotent producer only removes duplicates of its own internal retries, so with `max-attempts` > 1 consumers must
  de-duplicate (for example by `event_id`). Retries are off by default.
- **Invalid settings never stop the service.** A channel is switched off with an ERROR
  `kafka_publisher_channel_invalid channel=… reason=…` when:
  - `lanes` is outside 1-16;
  - `queue-capacity` is below the lane count, above 1,000,000, or above 250,000 per lane (each lane's queue is
    allocated up front);
  - `block-timeout` <= 0 under `BLOCK_WITH_TIMEOUT`;
  - `retry.max-attempts` < 1, `retry.multiplier` < 1, `retry.initial-backoff` or `retry.max-backoff` <= 0, or
    `retry.max-backoff` below `retry.initial-backoff`;
  - `delivery-timeout-ms` < `linger-ms` + `request-timeout-ms`, or `missing-topic-cooldown` is negative;
  - a value does not convert (for example an enum typo);
  - the Kafka client rejects the producer config, or building the channel fails in any other way (Errors included).

  Without a unique `KafkaTemplate`, enabled channels are switched off with ERROR
  `kafka_publisher_channel_disabled reason=no_unique_kafka_template`.
- **Topic check.** At startup, the maintenance thread describes the channel's `topics` (5 s bound) and logs
  ERROR `kafka_publisher_topic_missing` per missing topic. It never creates a topic.
- **Metrics** (only when the service has Micrometer and a `MeterRegistry` bean): `kafka.publisher.delivered` timer
  (channel, topic), `kafka.publisher.failed` counter (channel, topic, stage), `kafka.publisher.queue.depth` gauge (channel).
  A service's own `PublisherMetrics` bean replaces them. A failing metrics adapter or failure handler logs one WARN per
  interval (`kafka_publisher_metrics_failed`, `kafka_publisher_failure_handler_failed`) and a summary with the count.
- **Services with their own `@ComponentScan`.** The auto-configuration has no stereotype annotation, so a scan over
  `com.crp.system` cannot register it early: it loads only from `AutoConfiguration.imports` and still backs off for a
  service's own `EventPublisherChannels` bean.

### Missing topics cool down

When the producer cannot find a topic in the broker's metadata within `max-block-ms`, the send blocks its lane for that
long (2 s by default), and a lane carries all of its channel's topics. So the topic cools down instead:

- **Trigger.** The producer reports a `TimeoutException` (not a `BufferExhaustedException`, which is a full buffer)
  through the callback, inside `send()`: that is the metadata wait. No message matching.
- **Effect.** For `producer.missing-topic-cooldown` (default 30 s), sends to that topic fail at once, without calling
  the producer: `DELIVERY_FAILED`, cause `TopicCoolingDownException(topic, until)`, not retriable, `attempt` = the
  attempts made so far (0 for a first send). One WARN
  `kafka_publisher_topic_cooling_down channel=… topic=… for=…` marks the start. Other topics on the lane are not delayed.
- **Recovery.** The first send after the cool-down checks the topic again (one send per channel; the others keep
  failing fast meanwhile). If the topic is back, the cool-down ends; if not, it starts again. A cool-down that is over
  and that nobody sends to again is dropped at the next `summary-interval` tick, so a topic that is never used again
  keeps no entry; its next send is then treated like the first one.
- A topic the producer has never seen looks the same when the broker itself is unreachable at startup, so the same
  cool-down applies then. A delivery timeout reported later (`Expiring … record(s)`) does not start one.

### Failure stages

Every failure goes once through the channel's failure handlers (and fails the `publishWithResult` future):

| Stage | Raised on | Retried? | Logged |
|---|---|---|---|
| `SERIALIZATION` | caller | no | ERROR (rate-limited) |
| `QUEUE_FULL` | caller (or the maintenance thread, for a retry) | no | counted, one WARN summary per interval |
| `SEND_REJECTED` | lane: `producer.send` threw (closed producer, interrupt, non-API `KafkaException`) | if Kafka-retriable and attempts are left | ERROR (rate-limited) |
| `DELIVERY_FAILED` | producer callback: topic not in metadata after `max-block-ms`, buffer full, record too large, batch expired, not enough replicas, authorization; or, without a send, a topic cooling down (`TopicCoolingDownException`) | if Kafka-retriable and attempts are left (a cooling-down topic is not) | ERROR (rate-limited) |
| `CHANNEL_UNAVAILABLE` | caller: channel disabled, unknown or shutting down; or a record still queued when shutdown gave up | no | not logged (shutdown logs the count) |
| `INTERNAL` | any: a defect inside the publisher, a null topic, or a lane whose worker died twice | no | ERROR (rate-limited) |

The built-in logging handler logs the first failure per (channel, topic, stage) in each `summary-interval`, counts the
rest and logs `kafka_publish_failed_summary … count=N`. It never logs the payload (player data), and
`PublishFailure.toString()` shows only the payload's length. Its rate limit runs on the monotonic clock, so a wall-clock
step neither silences nor repeats an ERROR. Retries are off by default; when on, they are scheduled (no thread sleeps)
with `initial-backoff × multiplier^(n-2)` capped at `max-backoff`, and nothing is retried once the channel is closing.

### Threads

| Thread | Runs | Must never |
|---|---|---|
| caller | serialization, admission to a lane; under `CALLER_RUNS` also the send | throw; block, except under `CALLER_RUNS` or `BLOCK_WITH_TIMEOUT` (bounded) |
| `<channel>-publisher-<n>` (one per lane, daemon) | `producer.send`, bounded by `max-block-ms` | run business code |
| `kafka-producer-network-thread \| <app>-<channel>` | delivery callbacks: metrics, result futures, failure handlers | block |
| `kafka-publisher-maintenance` (only when a channel is enabled) | retries, summaries, the topic check | |

A disabled or invalid channel starts no thread and no producer, and the maintenance thread exists only while a channel
is enabled.

**Lane supervision.** If a lane's worker thread ever dies on something its guards could not catch, it is restarted once
(ERROR `kafka_publisher_lane_restarted`). If it dies again, or cannot be restarted, the lane stops accepting and fails its
queued records as `INTERNAL` (ERROR `kafka_publisher_lane_dead … abandoned=N`) instead of stranding them.

**Shutdown.** Every channel stops accepting at once and fails its pending retries. Then all lanes drain against one
shared deadline, the longest `shutdown-timeout`, so shutdown takes the longest timeout, not the sum. When the lanes
have drained or the deadline passes, each channel halts its lanes (no worker starts another record) and fails whatever
is still queued as `CHANNEL_UNAVAILABLE` before `close()` returns (WARN
`kafka_publisher_shutdown_abandoned channel=… count=N`, logged before the final summaries). Only then does it close its
producer with the time left, so nothing that was queued at the deadline is sent into a closing producer. Sends still
inside the producer return once it is closed; close waits up to 1 s more for them.

### Adding a failure handler

Any `PublishFailureHandler` bean is called for every channel, after the built-in logging handler (bean order applies).
The catalog collects the handlers when it is created, so a handler that publishes through a channel must look the
catalog up lazily (`ObjectProvider` or `@Lazy`); taking `EventPublisherChannels` in its constructor is a bean cycle
and the context fails at startup.

```kotlin
@Component
class DeadLetterFailureHandler(private val channels: ObjectProvider<EventPublisherChannels>) : PublishFailureHandler {
    private val deadLetters by lazy { channels.getObject().get("dead-letters") }

    // may run on the producer's I/O thread: never block here
    override fun onFailure(failure: PublishFailure) {
        if (failure.channel == deadLetters.channel || failure.stage != PublishFailureStage.DELIVERY_FAILED) return
        val payload = failure.payload ?: return
        deadLetters.publishJson("${failure.topic}.dlt", failure.key, payload, mapOf("failed_stage" to failure.stage.name))
    }
}
```

A handler that throws is logged and skipped; the next handler still runs. Events are best effort: anything still queued
when the process dies is lost. Must-deliver events need an outbox behind the same `EventPublisher` port.
