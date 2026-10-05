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
crypto.kafka.publisher.channels.reporting.queue-capacity=10000   # split evenly across the lanes
crypto.kafka.publisher.channels.reporting.backpressure=DROP      # DROP | CALLER_RUNS | BLOCK_WITH_TIMEOUT
crypto.kafka.publisher.channels.reporting.block-timeout=100ms    # BLOCK_WITH_TIMEOUT only
crypto.kafka.publisher.channels.reporting.ordering=PER_KEY       # PER_KEY | NONE
crypto.kafka.publisher.channels.reporting.retry.max-attempts=1   # 1 = only the producer's own retries
crypto.kafka.publisher.channels.reporting.retry.initial-backoff=200ms
crypto.kafka.publisher.channels.reporting.retry.multiplier=2.0
crypto.kafka.publisher.channels.reporting.retry.max-backoff=5s
crypto.kafka.publisher.channels.reporting.producer.max-block-ms=2000
crypto.kafka.publisher.channels.reporting.producer.buffer-memory=8388608
crypto.kafka.publisher.channels.reporting.producer.linger-ms=5
crypto.kafka.publisher.channels.reporting.producer.request-timeout-ms=10000
crypto.kafka.publisher.channels.reporting.producer.delivery-timeout-ms=30000   # >= linger + request timeout
crypto.kafka.publisher.channels.reporting.producer.acks=all      # idempotence is on only with acks=all
crypto.kafka.publisher.channels.reporting.producer.compression-type=none
crypto.kafka.publisher.channels.reporting.producer.extra.<any.producer.key>=<value>   # applied last
crypto.kafka.publisher.channels.reporting.topics=account.player.events,payments.request.events   # checked at startup, never created
crypto.kafka.publisher.channels.reporting.shutdown-timeout=5s
```

- **Producer.** Each enabled channel builds its own `KafkaProducer` from the service's one `KafkaTemplate` (its
  connection and security settings) plus the channel's limits. The client id is `<spring.application.name>-<channel>`.
  The channel never uses Kafka transactions and adds no `KafkaTemplate` or `ProducerFactory` bean.
- **Ordering.** Under `PER_KEY` the key's hash picks the lane, so one key keeps its order end to end. Two cases can still
  reorder one key: `CALLER_RUNS` under overload, and app retries (`max-attempts` > 1), which re-enter at the lane's tail.
- **Invalid settings never stop the service.** A channel is switched off with an ERROR
  `kafka_publisher_channel_invalid channel=… reason=…` when: lanes are outside 1-16, `queue-capacity` is below the
  lane count, `block-timeout` <= 0 under `BLOCK_WITH_TIMEOUT`, `retry.max-attempts` < 1, `retry.multiplier` < 1,
  `delivery-timeout-ms` < `linger-ms` + `request-timeout-ms`, a value does not convert (for example an enum typo), or the
  Kafka client rejects the producer config. Without a unique `KafkaTemplate`, enabled channels are switched off with
  ERROR `kafka_publisher_channel_disabled reason=no_unique_kafka_template`.
- **Topic check.** At startup, the maintenance thread describes the channel's `topics` (5 s bound) and logs
  ERROR `kafka_publisher_topic_missing` per missing topic. It never creates a topic.
- **Metrics** (only when the service has Micrometer and a `MeterRegistry` bean): `kafka.publisher.delivered` timer
  (channel, topic), `kafka.publisher.failed` counter (channel, topic, stage), `kafka.publisher.queue.depth` gauge (channel).
  A service's own `PublisherMetrics` bean replaces them.

### Failure stages

Every failure goes once through the channel's failure handlers (and fails the `publishWithResult` future):

| Stage | Raised on | Retried? | Logged |
|---|---|---|---|
| `SERIALIZATION` | caller | no | ERROR (rate-limited) |
| `QUEUE_FULL` | caller (or the maintenance thread, for a retry) | no | counted, one WARN summary per interval |
| `SEND_REJECTED` | lane: `producer.send` threw (closed producer, interrupt, non-API `KafkaException`) | if Kafka-retriable and attempts are left | ERROR (rate-limited) |
| `DELIVERY_FAILED` | producer callback: topic not in metadata after `max-block-ms`, buffer full, record too large, batch expired, not enough replicas, authorization | if Kafka-retriable and attempts are left | ERROR (rate-limited) |
| `CHANNEL_UNAVAILABLE` | caller: channel disabled, unknown or shutting down | no | not logged |
| `INTERNAL` | any: a defect inside the publisher | no | ERROR (rate-limited) |

The built-in logging handler logs the first failure per (channel, topic, stage) in each `summary-interval`, counts the
rest and logs `kafka_publish_failed_summary … count=N`. It never logs the payload (player data). Retries are off by
default; when on, they are scheduled (no thread sleeps) with `initial-backoff × multiplier^(n-2)` capped at
`max-backoff`, and nothing is retried once the channel is closing.

### Threads

| Thread | Runs | Must never |
|---|---|---|
| caller | serialization, admission to a lane; under `CALLER_RUNS` also the send | throw; block, except under `CALLER_RUNS` or `BLOCK_WITH_TIMEOUT` (bounded) |
| `<channel>-publisher-<n>` (one per lane, daemon) | `producer.send`, bounded by `max-block-ms` | run business code |
| `kafka-producer-network-thread \| <app>-<channel>` | delivery callbacks: metrics, result futures, failure handlers | block |
| `kafka-publisher-maintenance` (only when a channel is enabled) | retries, summaries, the topic check | |

A disabled or invalid channel starts no thread and no producer. On shutdown each channel stops accepting, fails its
pending retries, lets its lanes drain until `shutdown-timeout`, then closes its producer with the time left.

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
