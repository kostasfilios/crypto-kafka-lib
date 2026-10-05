# crypto-kafka-lib v1.3.0: a reusable async event publisher with an exception-handling layer

**Status:** APPROVED 2026-10-05 (owner).
- **Release:** tag `v1.3.0` in `kostasfilios/crypto-kafka-lib`, so the JitPack coordinates do not change, then mirror the commits to `AetherLabs-ExchangesSystem/crypto-kafka-lib`.
- **Property names and `reporting` defaults:** as below.
<!-- lifecycle: PROPOSED → APPROVED 2026-10-05 → IMPLEMENTED (PR #n) → RELEASED v1.3.0 <date> -->

## Problem (user view)

- **Player.** A Kafka or topic outage can freeze a bet, deposit or login for up to 60 s. Services send on the request thread, and the lib's producer waits that long for metadata (step 1, C12).
- **Engineer adding an event.** The lib only offers `KafkaMessageProducer.sendMessage(topic, message)`. It has no key, no protection for the caller, no failure handling and no switch, and it shares one buffer with the money path. Each service has therefore invented its own workaround, and some lose data silently: the Nixxe spool, B2BSync's dropped balance updates, and 13 copies of the reporting publisher.
- **This change.** One tested, reusable publisher in the lib:
  - named channels, each with its own producer and limits;
  - keyed sends;
  - no blocking of, and no exceptions thrown into, business code;
  - an exception-handling layer.

  Nothing changes for a service until it enables a channel, and `KafkaMessageProducer` is untouched.

## Why

The evidence below comes from the lib at v1.2.0 (f42651f), the version JitPack serves.

**The lib today**
- `KafkaMessageProducer.sendMessage` calls `KafkaTemplate.send(topic, data)`: no key, no headers, no try/catch.
- v1.2.0 added `sendMessageWithResult`, which is also keyless. It is the only change from v1.1.0 to v1.2.0.
- `KafkaProducerConfig.producerFactory()` sets only the bootstrap servers, String serializers and `max.request.size`. Every service therefore runs `max.block.ms` 60 000 and one shared 32 MiB `buffer.memory`.

**Users:** 34 PAM services. 58 build files use v1.1.0 and 3 use v1.2.0, counting the step-1 copies.

**Workarounds found in the 2026-10-05 reviews**
- Request-thread sends with the 60 s block:
  - the Deposit/Withdraw inline balance pushes;
  - the Nixxe `user-payment-*` sends;
  - the BackOfficeClientUser audit send.
- NixxeTransactions spool:
  - `PendingKafkaMessagesHandleJob.kt:89` re-spools exchange messages into the deposit file.
  - `JsonFileStorage.kt:70-72` and `:81-83` return before `unlock`.
  - `processPendingWithdrawMessages` loses a message when the async send fails.
- B2BSync `SideEffectsConfig` uses `AbortPolicy`, which drops FundsUpdates when its queue is full.
- The `ReportingEventPublisher` v2.3 copy sits in 13 services.

## What changes

- **New package.** `com.crp.system.libs.kafka.publisher`, released as **v1.3.0**. The change is additive.
- **Ports.**
  - Inbound: `EventPublisher` (send) and `EventPublisherChannels` (catalog).
  - Outbound: Kafka, serialization, failure handling, metrics and topic metadata.
- **Policies:** backpressure, ordering, failure classification and retries.
- **Configuration.** All of it is set per channel in `crypto.kafka.publisher.channels.<name>.*`.
- **Test double** for services: `RecordingEventPublisher`.

## Design

### Package layout

```
com.crp.system.libs.kafka.publisher
├── api/        public: the inbound ports, the extension ports, the DTOs services see
│               EventPublisher, EventPublisherChannels, EventSerializer, PublishFailureHandler,
│               PublisherMetrics, PublishFailureClassifier, RetryPolicy,
│               PublishResult, PublishFailure, PublishFailureStage, PublishFailedException, EventIds
├── core/       internal: the pipeline and its internal ports
│               ChannelEventPublisher, DisabledEventPublisher, FailureDispatcher,
│               PublishLanes, PublishLane, BackpressurePolicy (DropWhenFull, CallerRunsWhenFull, BlockWithTimeout),
│               RecordSender, TopicInspector, OutboundRecord, SendOutcome
├── adapters/   implementations of the outbound ports
│               KafkaRecordSender, ChannelProducerConfig, AdminClientTopicInspector, GsonEventSerializer,
│               KafkaRetriableClassifier, ExponentialRetryPolicy, LoggingPublishFailureHandler, MicrometerPublisherMetrics
├── spring/     wiring
│               EventPublisherAutoConfiguration, EventPublisherProperties (+ ChannelSettings, RetrySettings, ProducerSettings),
│               ConfiguredEventPublisherChannels
└── testing/    RecordingEventPublisher, RecordingEventPublisherChannels   (for service tests; no broker)
```

### Ports and communication

```
 service code                          crypto-kafka-lib (one ChannelEventPublisher per enabled channel)                                outside
 ────────────                          ─────────────────────────────────────────────────────────────────                                ───────
                         ┌───────────────── inbound ports ─────────────────┐
 @Component ──get(name)─►│ EventPublisherChannels (catalog)                 │
            ──publish──► │ EventPublisher                                   │
                         └──────────────┬──────────────────────────────────┘
                                        │ caller thread
                                        ▼
                         EventSerializer ─(json)─► OutboundRecord ─► BackpressurePolicy.admit(lane)
                                                                           │ queue full → QUEUE_FULL ─────────────┐
                                                                           ▼                                       │
                                                    PublishLane (worker thread, bounded FIFO)                       │
                                                                           │                                       │
                                                                           ▼                                       ▼
                                                    RecordSender.send ──► KafkaRecordSender ──► KafkaProducer ──► Kafka broker
                                                                           │ throws → SEND_REJECTED                ▲
                                                                           │ callback(error) → DELIVERY_FAILED      │ ack
                                                                           ▼                                       │
                                     PublishFailureClassifier ─ retriable + attempts left ─► RetryPolicy ─► maintenance thread ─► lane (again)
                                                                           │ final
                                                                           ▼
                                     FailureDispatcher ─► PublishFailureHandler chain (logging, metrics, any extra bean)
                                                       └► publishWithResult future: completeExceptionally(PublishFailedException)
                                     on ack ─► PublisherMetrics.delivered ─► publishWithResult future: complete(PublishResult)
```

| Port | Kind | Direction | Called by | Default adapter | Runs on |
|---|---|---|---|---|---|
| `EventPublisher` | inbound | service → lib | business code | `ChannelEventPublisher`, or `DisabledEventPublisher` when off | caller thread; returns at once |
| `EventPublisherChannels` | inbound | service → lib | service wiring | `ConfiguredEventPublisherChannels` | startup |
| `EventSerializer` | outbound, extension | lib → JSON | the pipeline | `GsonEventSerializer` | caller thread |
| `RecordSender` | outbound, internal | lib → Kafka | lane worker | `KafkaRecordSender` (`KafkaProducer`) | lane thread; the outcome arrives on the producer I/O thread |
| `PublishFailureHandler` | outbound, extension | lib → failure sinks | `FailureDispatcher` | `LoggingPublishFailureHandler`, plus any `PublishFailureHandler` bean | the thread where the failure happened |
| `PublisherMetrics` | outbound | lib → metrics | the pipeline | `MicrometerPublisherMetrics`, or `PublisherMetrics.None` | any |
| `TopicInspector` | outbound, internal | lib → Kafka admin | startup check | `AdminClientTopicInspector` | maintenance thread |
| `PublishFailureClassifier`, `RetryPolicy`, `BackpressurePolicy` | policy (strategy) | inside the core | the pipeline | Kafka-retriable / exponential / per settings | n/a |

**Data in and out:**
- **In:** `(topic, key, event | json, headers)`.
- **Out to Kafka:** `ProducerRecord<String, String>(topic, key, json, headers as UTF-8)`.
- **Out to handlers:** `PublishFailure`.
- **Out to `publishWithResult` callers:** `PublishResult`, or a `PublishFailedException`.

### Objects (DTOs, value objects, settings)

```kotlin
// ── api: what services and handlers see ──────────────────────────────────────────────────────────────
data class PublishResult(
    val channel: String,
    val topic: String,
    val key: String?,
    val partition: Int,
    val offset: Long,
    val attempts: Int,
    val latency: Duration,                  // from enqueue to the broker's acknowledgement
)

enum class PublishFailureStage {
    SERIALIZATION,        // caller thread: the event could not be turned into JSON
    QUEUE_FULL,           // caller thread: the lane's queue was full (DROP, or BLOCK_WITH_TIMEOUT after its wait)
    SEND_REJECTED,        // lane thread: producer.send threw (producer closed, interrupted, a non-API KafkaException)
    DELIVERY_FAILED,      // producer callback: topic not in metadata after max-block-ms, buffer full, record too large,
                          //   batch expired after delivery-timeout-ms, not enough replicas, authorization
    CHANNEL_UNAVAILABLE,  // caller thread: channel disabled, unknown or shutting down
    INTERNAL,             // any thread: a defect inside the publisher itself
}

data class PublishFailure(
    val channel: String,
    val topic: String,
    val key: String?,
    val stage: PublishFailureStage,
    val attempt: Int,                       // 0 = the record never reached Kafka
    val retriable: Boolean,
    val cause: Throwable?,
    val payload: String?,                   // the JSON when it exists; the logging handler never logs it
    val occurredAt: Instant,
)

class PublishFailedException(val failure: PublishFailure) :
    RuntimeException("publish failed: channel=${failure.channel} topic=${failure.topic} stage=${failure.stage}", failure.cause)

// ── core: internal value objects ──────────────────────────────────────────────────────────────────────
internal data class OutboundRecord(
    val channel: String,
    val topic: String,
    val key: String?,
    val payload: String,                    // JSON, serialized on the caller thread (snapshot)
    val headers: Map<String, String>,
    val enqueuedAt: Instant,
)

internal sealed interface SendOutcome {
    data class Delivered(val partition: Int, val offset: Long) : SendOutcome
    data class Failed(val error: Exception) : SendOutcome
}

// ── spring: settings bound from crypto.kafka.publisher.* (Kotlin defaults apply to missing keys) ──────
@ConfigurationProperties("crypto.kafka.publisher")
data class EventPublisherProperties(
    val summaryInterval: Duration = Duration.ofSeconds(10),       // failure/drop summaries
    val channels: Map<String, ChannelSettings> = emptyMap(),
)

data class ChannelSettings(
    val enabled: Boolean = false,
    val serializer: SerializerNaming = SerializerNaming.SNAKE_CASE,
    val lanes: Int = 2,
    val queueCapacity: Int = 10_000,                               // split evenly across the lanes
    val backpressure: BackpressureMode = BackpressureMode.DROP,
    val blockTimeout: Duration = Duration.ofMillis(100),          // BLOCK_WITH_TIMEOUT only
    val ordering: OrderingMode = OrderingMode.PER_KEY,
    val retry: RetrySettings = RetrySettings(),
    val producer: ProducerSettings = ProducerSettings(),
    val topics: List<String> = emptyList(),                       // checked once at startup; never created
    val shutdownTimeout: Duration = Duration.ofSeconds(5),
)

data class RetrySettings(
    val maxAttempts: Int = 1,                                      // 1 = only the producer's own retries
    val initialBackoff: Duration = Duration.ofMillis(200),
    val multiplier: Double = 2.0,
    val maxBackoff: Duration = Duration.ofSeconds(5),
)

data class ProducerSettings(
    val maxBlockMs: Long = 2_000,
    val bufferMemory: Long = 8L * 1024 * 1024,
    val lingerMs: Int = 5,
    val requestTimeoutMs: Int = 10_000,
    val deliveryTimeoutMs: Int = 30_000,                           // must be ≥ linger + request timeout
    val acks: String = "all",
    val compressionType: String = "none",
    val extra: Map<String, String> = emptyMap(),                   // any other ProducerConfig key, applied last
)

enum class SerializerNaming { SNAKE_CASE, IDENTITY }
enum class BackpressureMode { DROP, CALLER_RUNS, BLOCK_WITH_TIMEOUT }
enum class OrderingMode { PER_KEY, NONE }
```

### Ports (interfaces)

```kotlin
// ── api: inbound ports ────────────────────────────────────────────────────────────────────────────────
/** The only thing business code calls. Never throws. Returns at once (CALLER_RUNS / BLOCK_WITH_TIMEOUT aside). */
interface EventPublisher {
    val channel: String
    fun publish(topic: String, key: String?, event: Any, headers: Map<String, String> = emptyMap())
    fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String> = emptyMap())
    fun publishWithResult(topic: String, key: String?, event: Any, headers: Map<String, String> = emptyMap()): CompletableFuture<PublishResult>
}

interface EventPublisherChannels {
    /** An unknown or disabled channel returns a publisher that does nothing; an unknown name also logs one WARN. */
    fun get(name: String): EventPublisher
    fun names(): Set<String>
}

// ── api: outbound extension ports ─────────────────────────────────────────────────────────────────────
fun interface EventSerializer { fun serialize(event: Any): String }

/** The end of the exception-handling layer. May run on the producer I/O thread: must not block. */
fun interface PublishFailureHandler { fun onFailure(failure: PublishFailure) }

fun interface PublishFailureClassifier { fun isRetriable(error: Throwable): Boolean }

/** Returns the delay before [nextAttempt], or null to stop retrying. */
fun interface RetryPolicy { fun delayBeforeAttempt(nextAttempt: Int): Duration? }

interface PublisherMetrics {
    fun delivered(channel: String, topic: String, latency: Duration)
    fun failed(channel: String, topic: String, stage: PublishFailureStage)
    fun queueDepth(channel: String, depth: () -> Int)
    object None : PublisherMetrics {
        override fun delivered(channel: String, topic: String, latency: Duration) = Unit
        override fun failed(channel: String, topic: String, stage: PublishFailureStage) = Unit
        override fun queueDepth(channel: String, depth: () -> Int) = Unit
    }
}

// ── core: internal ports ──────────────────────────────────────────────────────────────────────────────
/** `send` may throw (→ SEND_REJECTED); otherwise `onOutcome` is called exactly once. */
internal interface RecordSender {
    fun send(record: OutboundRecord, onOutcome: (SendOutcome) -> Unit)
    fun close(timeout: Duration)
}

internal fun interface TopicInspector { fun missingTopics(topics: Collection<String>, timeout: Duration): Set<String> }

internal fun interface BackpressurePolicy { fun admit(lane: PublishLane, task: Runnable): Boolean }
```

### Core: the channel pipeline

```kotlin
internal class ChannelEventPublisher(
    override val channel: String,
    private val settings: ChannelSettings,
    private val serializer: EventSerializer,
    private val sender: RecordSender,
    private val lanes: PublishLanes,
    private val backpressure: BackpressurePolicy,
    private val classifier: PublishFailureClassifier,
    private val retryPolicy: RetryPolicy,
    private val maintenance: ScheduledExecutorService,      // retries, summaries, topic check
    private val failures: FailureDispatcher,
    private val metrics: PublisherMetrics,
    private val clock: Clock,
) : EventPublisher {

    private val open = AtomicBoolean(true)

    override fun publish(topic: String, key: String?, event: Any, headers: Map<String, String>) =
        accept(topic, key, headers, result = null) { serializer.serialize(event) }

    override fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String>) =
        accept(topic, key, headers, result = null) { json }

    override fun publishWithResult(topic: String, key: String?, event: Any, headers: Map<String, String>) =
        CompletableFuture<PublishResult>().also { result -> accept(topic, key, headers, result) { serializer.serialize(event) } }

    /** Caller thread. Never throws. */
    private fun accept(
        topic: String, key: String?, headers: Map<String, String>,
        result: CompletableFuture<PublishResult>?, payload: () -> String,
    ) {
        try {
            if (!open.get()) return failures.fail(CHANNEL_UNAVAILABLE, topic, key, attempt = 0, cause = null, payload = null, result)
            val json = try {
                payload()
            } catch (e: Exception) {
                return failures.fail(SERIALIZATION, topic, key, attempt = 0, cause = e, payload = null, result)
            }
            val record = OutboundRecord(channel, topic, key, json, headers, clock.instant())
            if (!backpressure.admit(lanes.forKey(key)) { send(record, attempt = 1, result) }) {
                failures.fail(QUEUE_FULL, topic, key, attempt = 0, cause = null, payload = json, result)
            }
        } catch (e: Exception) {
            failures.fail(INTERNAL, topic, key, attempt = 0, cause = e, payload = null, result)
        }
    }

    /** Lane thread (the caller under CALLER_RUNS; the maintenance thread for a retry). */
    private fun send(record: OutboundRecord, attempt: Int, result: CompletableFuture<PublishResult>?) {
        try {
            sender.send(record) { outcome ->
                when (outcome) {
                    is SendOutcome.Delivered -> {
                        val latency = Duration.between(record.enqueuedAt, clock.instant())
                        metrics.delivered(channel, record.topic, latency)
                        result?.complete(PublishResult(channel, record.topic, record.key, outcome.partition, outcome.offset, attempt, latency))
                    }
                    is SendOutcome.Failed -> retryOrFail(DELIVERY_FAILED, record, attempt, outcome.error, result)
                }
            }
        } catch (e: Exception) {
            retryOrFail(SEND_REJECTED, record, attempt, e, result)
        }
    }

    private fun retryOrFail(
        stage: PublishFailureStage, record: OutboundRecord, attempt: Int,
        error: Exception, result: CompletableFuture<PublishResult>?,
    ) {
        val retriable = classifier.isRetriable(error)
        val delay = if (retriable && open.get()) retryPolicy.delayBeforeAttempt(attempt + 1) else null
        if (delay != null) {
            try {
                maintenance.schedule({
                    if (!backpressure.admit(lanes.forKey(record.key)) { send(record, attempt + 1, result) }) {
                        failures.fail(QUEUE_FULL, record.topic, record.key, attempt, error, record.payload, result, retriable)
                    }
                }, delay.toMillis(), TimeUnit.MILLISECONDS)
                return
            } catch (shuttingDown: RejectedExecutionException) {
                // fall through: report the original failure
            }
        }
        failures.fail(stage, record.topic, record.key, attempt, error, record.payload, result, retriable)
    }

    /** Stop accepting, let the lanes drain until the deadline, then close the producer with the time left. */
    fun close() {
        if (!open.compareAndSet(true, false)) return
        val deadline = clock.instant().plus(settings.shutdownTimeout)
        lanes.drainAndStop(deadline)
        sender.close(Duration.between(clock.instant(), deadline).coerceAtLeast(Duration.ZERO))
    }
}

/** Every failure goes through here exactly once: metrics, then each handler, then the caller's future. */
internal class FailureDispatcher(
    private val channel: String,
    private val handlers: List<PublishFailureHandler>,
    private val metrics: PublisherMetrics,
    private val clock: Clock,
) {
    fun fail(
        stage: PublishFailureStage, topic: String, key: String?, attempt: Int, cause: Throwable?,
        payload: String?, result: CompletableFuture<PublishResult>?, retriable: Boolean = false,
    ) {
        val failure = PublishFailure(channel, topic, key, stage, attempt, retriable, cause, payload, clock.instant())
        guarded("metrics") { metrics.failed(channel, topic, stage) }
        for (handler in handlers) guarded(handler.javaClass.simpleName) { handler.onFailure(failure) }
        result?.completeExceptionally(PublishFailedException(failure))
    }

    private inline fun guarded(what: String, block: () -> Unit) {
        try {
            block()
        } catch (e: Exception) {
            logger.warn("kafka_publisher_failure_handler_failed channel={} handler={}: {}", channel, what, e.toString())
        }
    }
}

/** What `EventPublisherChannels.get` returns for a disabled or unknown channel. */
internal class DisabledEventPublisher(override val channel: String, private val clock: Clock) : EventPublisher {
    override fun publish(topic: String, key: String?, event: Any, headers: Map<String, String>) = Unit
    override fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String>) = Unit
    override fun publishWithResult(topic: String, key: String?, event: Any, headers: Map<String, String>) =
        CompletableFuture.failedFuture<PublishResult>(PublishFailedException(
            PublishFailure(channel, topic, key, CHANNEL_UNAVAILABLE, 0, false, null, null, clock.instant())))
}
```

### Core: lanes and backpressure

```kotlin
/** PER_KEY: the key's hash picks the lane, so one key always uses one FIFO lane. NONE or a null key: round-robin. */
internal class PublishLanes(channel: String, count: Int, totalCapacity: Int, private val ordering: OrderingMode) {
    private val lanes = List(count) { index -> PublishLane("$channel-publisher-$index", (totalCapacity + count - 1) / count) }
    private val next = AtomicInteger()

    fun forKey(key: String?): PublishLane =
        if (ordering == OrderingMode.PER_KEY && key != null) lanes[Math.floorMod(key.hashCode(), lanes.size)]
        else lanes[Math.floorMod(next.getAndIncrement(), lanes.size)]

    fun queued(): Int = lanes.sumOf { it.queued() }

    fun drainAndStop(deadline: Instant) {
        lanes.forEach { it.stopAccepting() }
        lanes.forEach { it.awaitDrained(deadline) }
    }
}

/** One worker thread over one bounded queue. JDK only: no Reactor in the lib. */
internal class PublishLane(name: String, capacity: Int) {
    private val queue = ArrayBlockingQueue<Runnable>(capacity)
    @Volatile private var accepting = true
    private val worker = Thread(::work, name).apply { isDaemon = true; start() }

    fun tryEnqueue(task: Runnable): Boolean = accepting && queue.offer(task)
    fun enqueueWaiting(task: Runnable, timeout: Duration): Boolean =
        accepting && queue.offer(task, timeout.toNanos(), TimeUnit.NANOSECONDS)
    fun queued(): Int = queue.size
    fun stopAccepting() { accepting = false }
    fun awaitDrained(deadline: Instant) = worker.join(Duration.between(Instant.now(), deadline).toMillis().coerceAtLeast(1))

    private fun work() {
        while (accepting || queue.isNotEmpty()) {
            val task = queue.poll(100, TimeUnit.MILLISECONDS) ?: continue
            try { task.run() } catch (e: Exception) { /* tasks guard themselves; this keeps the lane alive */ }
        }
    }
}

internal object DropWhenFull : BackpressurePolicy {
    override fun admit(lane: PublishLane, task: Runnable) = lane.tryEnqueue(task)
}

internal object CallerRunsWhenFull : BackpressurePolicy {
    override fun admit(lane: PublishLane, task: Runnable): Boolean {
        if (!lane.tryEnqueue(task)) task.run()        // the caller pays; same-key order can break here (AD3)
        return true
    }
}

internal class BlockWithTimeout(private val timeout: Duration) : BackpressurePolicy {
    override fun admit(lane: PublishLane, task: Runnable) = lane.enqueueWaiting(task, timeout)
}
```

### Adapters: Kafka

```kotlin
internal class KafkaRecordSender(private val producer: Producer<String, String>) : RecordSender {
    override fun send(record: OutboundRecord, onOutcome: (SendOutcome) -> Unit) {
        val headers = record.headers.map { (name, value) -> RecordHeader(name, value.toByteArray(Charsets.UTF_8)) }
        producer.send(ProducerRecord(record.topic, null, record.key, record.payload, headers)) { metadata, error ->
            onOutcome(if (error != null) SendOutcome.Failed(error) else SendOutcome.Delivered(metadata.partition(), metadata.offset()))
        }
    }

    override fun close(timeout: Duration) = producer.close(timeout)
}

/** The channel's producer config: the service's connection settings (from the one KafkaTemplate) + the channel's limits. */
internal object ChannelProducerConfig {
    fun build(shared: Map<String, Any>, channel: String, settings: ProducerSettings, applicationName: String): Map<String, Any> =
        HashMap(shared).apply {
            remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG)                      // never transactional (AD9)
            put(ProducerConfig.CLIENT_ID_CONFIG, "$applicationName-$channel")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, settings.acks)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, settings.acks == "all" || settings.acks == "-1")
            put(ProducerConfig.MAX_BLOCK_MS_CONFIG, settings.maxBlockMs)
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.bufferMemory)
            put(ProducerConfig.LINGER_MS_CONFIG, settings.lingerMs)
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, settings.requestTimeoutMs)
            put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, settings.deliveryTimeoutMs)
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, settings.compressionType)
            putAll(settings.extra)
        }
}
```

kafka-clients 3.x reports most send errors through the callback, not as a throw. That includes a topic missing from metadata after `max-block-ms`, a full buffer and a record that is too large. Those errors are therefore `DELIVERY_FAILED`. `SEND_REJECTED` covers what `send` really throws: a closed producer, an interrupt, or a non-API `KafkaException`.

### The exception-handling layer

| Stage | Raised on | Classified | Then |
|---|---|---|---|
| `SERIALIZATION` | caller | final | handlers |
| `QUEUE_FULL` | caller (or maintenance, on a retry) | final | handlers; logged only as a summary |
| `SEND_REJECTED` | lane | `KafkaRetriableClassifier` | retry if retriable with attempts left, otherwise handlers |
| `DELIVERY_FAILED` | lane (immediate) or producer I/O | `KafkaRetriableClassifier` | retry if retriable with attempts left, otherwise handlers |
| `CHANNEL_UNAVAILABLE` | caller | final | the caller's future only; not logged |
| `INTERNAL` | any | final | handlers |

```kotlin
object KafkaRetriableClassifier : PublishFailureClassifier {
    override fun isRetriable(error: Throwable): Boolean =
        generateSequence(error) { it.cause }.take(10).any { it is org.apache.kafka.common.errors.RetriableException }
}

class ExponentialRetryPolicy(private val settings: RetrySettings) : RetryPolicy {
    override fun delayBeforeAttempt(nextAttempt: Int): Duration? {
        if (nextAttempt > settings.maxAttempts) return null
        val millis = settings.initialBackoff.toMillis() * settings.multiplier.pow(nextAttempt - 2)
        return Duration.ofMillis(millis.toLong().coerceAtMost(settings.maxBackoff.toMillis()))
    }
}

/**
 * Logs the first failure per (channel, topic, stage) in each summary interval and counts the rest.
 * Drops are only counted. Never logs payloads (player data).
 */
class LoggingPublishFailureHandler(private val summaryInterval: Duration, private val clock: Clock) : PublishFailureHandler {
    private data class SummaryKey(val channel: String, val topic: String, val stage: PublishFailureStage)
    private val lastLoggedAt = ConcurrentHashMap<SummaryKey, AtomicLong>()
    private val suppressed = ConcurrentHashMap<SummaryKey, LongAdder>()

    override fun onFailure(failure: PublishFailure) {
        if (failure.stage == CHANNEL_UNAVAILABLE) return
        val key = SummaryKey(failure.channel, failure.topic, failure.stage)
        val now = clock.millis()
        val last = lastLoggedAt.computeIfAbsent(key) { AtomicLong(0) }
        val previous = last.get()
        if (failure.stage != QUEUE_FULL && now - previous >= summaryInterval.toMillis() && last.compareAndSet(previous, now)) {
            logger.error(
                "kafka_publish_failed channel={} topic={} key={} stage={} attempt={} retriable={} cause={}",
                failure.channel, failure.topic, failure.key, failure.stage, failure.attempt, failure.retriable, causeChain(failure.cause),
            )
        } else {
            suppressed.computeIfAbsent(key) { LongAdder() }.increment()
        }
    }

    /** Run every summary interval by the maintenance thread, and once at shutdown. */
    fun flushSummaries() = suppressed.forEach { (key, counter) ->
        val count = counter.sumThenReset()
        if (count > 0) logger.warn(
            "kafka_publish_failed_summary channel={} topic={} stage={} count={} interval={}",
            key.channel, key.topic, key.stage, count, summaryInterval,
        )
    }

    private fun causeChain(error: Throwable?) =
        generateSequence(error) { it.cause }.take(5).joinToString(" <- ") { "${it.javaClass.simpleName}: ${it.message}" }
}
```

**Extension points.** A service adds a `PublishFailureHandler` bean, and every channel calls it after the built-in ones. Planned uses:
- a dead-letter topic handler, which republishes `failure.payload` through another channel;
- a durable spool handler, to replace the Nixxe `JsonFileStorage` path.

A must-deliver outbox would be another `EventPublisher` implementation behind the same port.

### Wiring: properties, catalog, auto-configuration

```properties
crypto.kafka.publisher.summary-interval=10s
crypto.kafka.publisher.channels.reporting.enabled=${reporting.events.enabled:false}
crypto.kafka.publisher.channels.reporting.serializer=SNAKE_CASE
crypto.kafka.publisher.channels.reporting.lanes=2
crypto.kafka.publisher.channels.reporting.queue-capacity=10000
crypto.kafka.publisher.channels.reporting.backpressure=DROP
crypto.kafka.publisher.channels.reporting.ordering=PER_KEY
crypto.kafka.publisher.channels.reporting.retry.max-attempts=1
crypto.kafka.publisher.channels.reporting.producer.max-block-ms=2000
crypto.kafka.publisher.channels.reporting.producer.buffer-memory=8388608
crypto.kafka.publisher.channels.reporting.producer.linger-ms=5
crypto.kafka.publisher.channels.reporting.producer.delivery-timeout-ms=30000
crypto.kafka.publisher.channels.reporting.topics=account.player.events,payments.request.events
crypto.kafka.publisher.channels.reporting.shutdown-timeout=5s
# later, e.g. balance pushes that must never drop:
# crypto.kafka.publisher.channels.balance-updates.enabled=true
# crypto.kafka.publisher.channels.balance-updates.serializer=IDENTITY
# crypto.kafka.publisher.channels.balance-updates.backpressure=CALLER_RUNS
```

```kotlin
@AutoConfiguration(afterName = ["org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration"])
@ConditionalOnClass(KafkaTemplate::class)
@EnableConfigurationProperties(EventPublisherProperties::class)
class EventPublisherAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(EventPublisherChannels::class)
    fun eventPublisherChannels(
        properties: EventPublisherProperties,
        kafkaTemplate: ObjectProvider<KafkaTemplate<String, String>>,
        extraFailureHandlers: ObjectProvider<PublishFailureHandler>,
        metrics: ObjectProvider<PublisherMetrics>,
        environment: Environment,
    ) = ConfiguredEventPublisherChannels.create(
        properties = properties,
        sharedProducerConfig = kafkaTemplate.ifUnique?.producerFactory?.configurationProperties,   // null → channels off + ERROR
        extraFailureHandlers = extraFailureHandlers.orderedStream().toList(),
        metrics = metrics.ifAvailable ?: PublisherMetrics.None,
        applicationName = environment.getProperty("spring.application.name", "pam"),
        clock = Clock.systemUTC(),
    )

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["io.micrometer.core.instrument.MeterRegistry"])
    class MicrometerMetricsConfiguration {
        @Bean
        @ConditionalOnBean(type = ["io.micrometer.core.instrument.MeterRegistry"])
        fun micrometerPublisherMetrics(registry: MeterRegistry): PublisherMetrics = MicrometerPublisherMetrics(registry)
    }
}
```

The `AutoConfiguration.imports` file lists `EventPublisherAutoConfiguration`. Because `@SpringBootApplication`'s scan skips auto-configurations, it is registered once even in services whose scan covers `com.crp.system.libs.kafka`.

`ConfiguredEventPublisherChannels.create` decides each channel as follows:

| Channel state | Result |
|---|---|
| `enabled=false` | `DisabledEventPublisher`, plus one INFO `kafka_publisher_channel_disabled` |
| no unique `KafkaTemplate` | `DisabledEventPublisher`, plus ERROR `kafka_publisher_channel_disabled reason=no_unique_kafka_template` |
| invalid settings | `DisabledEventPublisher`, plus ERROR `kafka_publisher_channel_invalid reason=…`. The service still starts (AD10). Invalid means any of:<br>• lanes outside 1–16<br>• queue capacity below the lane count<br>• `block-timeout` ≤ 0 under BLOCK_WITH_TIMEOUT<br>• `max-attempts` < 1<br>• `multiplier` < 1<br>• `delivery-timeout-ms` < `linger-ms` + `request-timeout-ms` |
| otherwise | `ChannelEventPublisher` with:<br>• its own `KafkaProducer` (`ChannelProducerConfig`)<br>• `PublishLanes`<br>• the backpressure policy<br>• `KafkaRetriableClassifier`<br>• `ExponentialRetryPolicy`<br>• `FailureDispatcher` (logging handler + metrics + extra handlers)<br>• its queue-depth gauge<br>• a one-off topic check on the maintenance thread (`AdminClientTopicInspector`, 5 s bound, ERROR `kafka_publisher_topic_missing` per missing topic) |

- **The maintenance thread** (`kafka-publisher-maintenance`, a single-thread scheduler) exists only when at least one channel is enabled. It runs retries, `flushSummaries` every `summary-interval`, and the topic checks.
- **`close()`** closes each channel (drain, then producer close), stops the maintenance thread and flushes the summaries once more.

### Threads

| Thread | Runs | Must never |
|---|---|---|
| caller (request / consumer thread) | serialization, `OutboundRecord`, admission to a lane; under CALLER_RUNS also the send | throw; block, except under CALLER_RUNS or BLOCK_WITH_TIMEOUT, which are bounded |
| lane worker `<channel>-publisher-<n>` (one per lane) | `producer.send`, bounded by `max-block-ms`; failures the producer reports immediately | run business code |
| producer I/O `kafka-producer-network-thread \| <app>-<channel>` | delivery callbacks: metrics, result futures, failure handlers | block. Handlers and `publishWithResult` continuations must be quick or use `…Async(executor)`. |
| maintenance `kafka-publisher-maintenance` (only when a channel is enabled) | retries, summaries, topic check | n/a |

### Service usage: before and after (illustrative)

```kotlin
// before: the v2.3 copy (~200 lines + tests) in each of 13 services
@Component
class SelfExclusionReportingEvents(
    private val publisher: ReportingEventPublisher,
    @Value("\${reporting.events.topic.responsible-gambling}") private val topic: String,
) {
    fun lifted(event: SelfExclusionReportingEvent) = publisher.publish(topic, event.playerId.toString(), event)
}

// after: lib v1.3.0; the copy and its tests are deleted, and ReportingEventIds becomes EventIds
@Component
class SelfExclusionReportingEvents(
    channels: EventPublisherChannels,
    @Value("\${reporting.events.topic.responsible-gambling}") private val topic: String,
) {
    private val reporting = channels.get("reporting")
    fun lifted(event: SelfExclusionReportingEvent) = reporting.publish(topic, event.playerId.toString(), event)
}

// service tests: no broker
val channels = RecordingEventPublisherChannels()
SelfExclusionReportingEvents(channels, "account.responsible_gambling.events").lifted(event)
assertThat(channels.recorded("reporting").single().key).isEqualTo("42")
```

`EventIds` is ported unchanged from the reporting copy, so the golden ids stay identical:
- `nameBased(name)` gives UUIDv5 in the OID namespace;
- `random()` gives v4;
- `utcMillis(instant)` formats `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`.

## What does NOT change

- **Existing lib classes.** `KafkaMessageProducer`, `KafkaProducerConfig`, `KafkaConsumerConfig` and the `kafkaTemplate` bean are unchanged. Upgrading changes no service's behaviour until it declares and enables a channel.
- **Kafka itself.** No topic creation, no broker settings, no consumer changes.
- **No outbox.** This publisher is best-effort: events still queued at a crash are lost. Must-deliver events need an outbox, which fits behind `EventPublisher` later.
- **Existing fragile sends** are not migrated here: the inline balance pushes, the Nixxe spool, B2BSync's executor, and the audit sends. Each gets its own proposal (Step 4).

## Architecture decisions (high throughput)

| # | Decision | Why | Cost if wrong |
|---|---|---|---|
| AD1 | One worker thread per lane over an `ArrayBlockingQueue`. JDK only, no Reactor. | • The caller does an O(1) offer.<br>• Memory is bounded.<br>• No reactor-core version coupling across 34 services. | Same semantics as v2.3's `boundedElastic(2, 10 000)`; internal to change. |
| AD2 | One Kafka producer per channel: not per topic, not the shared one. | • It isolates `max.block.ms` and `buffer.memory` from the money path.<br>• A producer is thread-safe and batches across topics. | One more connection set per broker per channel (one broker today). |
| AD3 | `PER_KEY` lanes: the key's hash picks the lane. | • Same-key events stay in order end to end, because the idempotent producer keeps per-partition order.<br>• A stuck lane blocks only its keys. | Two cases can still reorder same-key events:<br>• CALLER_RUNS under overload;<br>• app retries, which re-enter at the tail.<br>Channels that need strict order keep `max-attempts=1` and avoid CALLER_RUNS. |
| AD4 | Serialize on the caller thread. | Snapshot semantics, and tens of µs for small events. | Large events go through `publishJson`. |
| AD5 | Backpressure per channel: DROP, CALLER_RUNS or BLOCK_WITH_TIMEOUT. | Analytics and balance pushes need different policies. Today B2BSync drops balances. | A property change. |
| AD6 | Retries are off by default. When on, they apply only to retriable failures and are scheduled, so no thread sleeps. | The producer already retries until `delivery.timeout.ms`. | Off by default, so behaviour equals v2.3. |
| AD7 | Producer defaults: `max.block.ms` 2000, 8 MiB buffer, `linger.ms` 5, `acks=all`, idempotence on, `delivery.timeout.ms` 30 000, no compression. | The cost visible to the caller is bounded, with Kafka's durability defaults. | Each is a property. |
| AD8 | Memory per channel is about `queue-capacity` × event size + `buffer-memory` (≈ 18 MiB). | Predictable heap. | Lower the queue for big events. |
| AD9 | No `KafkaTemplate` or `ProducerFactory` beans, and no Kafka transactions. | • It keeps the one-template rule.<br>• kafka-go ignores aborted-transaction markers. | n/a |
| AD10 | Invalid channel settings disable that channel with an ERROR. They never stop the service. | A typo in a reporting property must not take a money service down. | The channel is off until fixed; the ERROR and metrics show it. |
| AD11 | Failure handlers and result continuations may run on the producer I/O thread. | No thread hop on the hot path. | A slow handler delays acknowledgements for that producer; handlers are documented as non-blocking. |

## Rollout (priority, high → low)

| # | Step | Where | Agent wall-clock | Depends on |
|---|---|---|---|---|
| 1 | Build v1.3.0 to this design. Tests: unit, Spring context, and Redpanda integration (skipped without a broker). | `crypto-kafka-lib`, branch `feat/event-publisher` from v1.2.0 | ~1–1.5 h incl. review | approved |
| 2 | Release: PR + merge in `kostasfilios/crypto-kafka-lib`, tag `v1.3.0`, check the JitPack build log, mirror to the org copy. | GitHub + JitPack | ~10 min | 1 |
| 3 | Migrate the 13 PAM reporting branches to the `reporting` channel. This replaces the v2.3 sync sweep. | the PAM reporting PRs | ~1 h in 3 parallel lanes | 2 |
| 4 | Later, one proposal each: B2BSync balance updates (CALLER_RUNS), the Nixxe spool, inline `user-payment-*` sends, audit sends. | per service | per proposal | 3 |

## Risks & rollback

- **The JitPack build fails.** Nothing changes for the services; fix it and tag v1.3.1.
- **Upgrading from v1.1.0.** v1.2.0 only added `sendMessageWithResult`. v1.3.0 adds a package and an auto-configuration that is inert without channel properties.
- **Auto-configuration.** It registers one bean, `EventPublisherChannels`, and no Kafka beans. A context test proves `kafkaTemplate` stays the only template.
- **A misconfigured channel** shows up in the ERROR at startup, in the summaries and in the metrics. The fix is a property change.
- **Rollback.** Per service, turn the channel off, or pin the previous lib version and redeploy.

## Verification

**Unit tests with a fake `RecordSender`**
- **Caller safety:**
  - no stage throws to the caller;
  - the caller is never blocked by a sender stuck in `send`.
- **Backpressure:**
  - DROP gives one summary, with the count;
  - CALLER_RUNS runs on the caller;
  - BLOCK_WITH_TIMEOUT waits, then reports QUEUE_FULL.
- **Ordering:**
  - PER_KEY keeps order with a stuck lane;
  - NONE spreads load across lanes.
- **Retries:**
  - only retriable errors are retried, with exponential delays, bounded by `max-attempts`;
  - nothing is retried during shutdown.
- **Failure handling:**
  - `FailureDispatcher`: a throwing handler does not break the chain, and the future is completed exactly once;
  - the logging handler never logs the payload, and it rate-limits per (channel, topic, stage) and flushes summaries;
  - each stage maps correctly, with `publishWithResult` futures completed with `PublishResult` or `PublishFailedException`.
- **Lifecycle:**
  - a disabled or invalid channel starts no threads and no producer;
  - shutdown drains within the deadline, then closes.
- **`ChannelProducerConfig`:** keeps the connection settings, applies the limits and `client.id`, removes `transactional.id`, and sets idempotence only with `acks=all`.
- **Serializers and ids:**
  - `GsonEventSerializer`: snake_case or identity naming, nulls kept, no HTML escaping;
  - `EventIds`: golden values equal the reporting contract's.

**Other checks**
- **Spring context tests:**
  - with the lib's `KafkaProducerConfig`, exactly one `KafkaTemplate` exists;
  - channels bind from properties, with Kotlin defaults filling missing keys;
  - the Micrometer adapter is used only when a `MeterRegistry` exists;
  - the placeholder `${reporting.events.enabled:false}` works.
- **Redpanda integration test** (set `KAFKA_PUBLISHER_TEST_BOOTSTRAP`; skipped when unset):
  - keyed delivery with headers;
  - a missing topic costs the caller nothing and ends as `DELIVERY_FAILED` ("not present in metadata after 2000 ms").
- **Mutation pass** over the pipeline and the policies.

**Done means:** v1.3.0 resolves from JitPack, and AccountServices builds and passes its tests on it before the others migrate.
