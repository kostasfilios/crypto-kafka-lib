package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.EventPublisher
import com.crp.system.libs.kafka.publisher.api.EventSerializer
import com.crp.system.libs.kafka.publisher.api.PublishFailureClassifier
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.INTERNAL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.SEND_REJECTED
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.SERIALIZATION
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.api.RetryPolicy
import com.crp.system.libs.kafka.publisher.api.TopicCoolingDownException
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/** One enabled channel: serialize on the caller, queue on a lane, send on the lane, classify and retry or fail. */
internal class ChannelEventPublisher(
    override val channel: String,
    private val settings: ChannelSettings,
    private val serializer: EventSerializer,
    private val sender: RecordSender,
    private val lanes: PublishLanes,
    private val backpressure: BackpressurePolicy,
    private val classifier: PublishFailureClassifier,
    private val retryPolicy: RetryPolicy,
    private val maintenance: ScheduledExecutorService, // retries, summaries, topic check
    private val failures: FailureDispatcher,
    private val metrics: PublisherMetrics,
    private val clock: Clock,
    /** Monotonic time for latency and cool-downs. Waits (shutdown) always use the real `System.nanoTime()`. */
    private val nanoTime: () -> Long,
    private val warnInterval: Duration,
) : EventPublisher {

    private val open = AtomicBoolean(true)
    private val pendingRetries: MutableSet<PendingRetry> = ConcurrentHashMap.newKeySet()
    private val metricsWarnings = RateLimiter<String>(warnInterval, nanoTime)
    private val cooldownNanos = settings.producer.missingTopicCooldown.saturatedNanos()

    /** Records given up as CHANNEL_UNAVAILABLE at shutdown: by close(), or by a lane worker that had just polled one as the lanes halted. */
    private val abandonedAtShutdown = AtomicInteger()

    /**
     * Topic -> monotonic time until which sends to it fail at once: it was missing from the broker's metadata. An entry
     * that is over stays for the next send to the topic (the probe), or goes in [expireCooldowns].
     */
    private val coolingDown = ConcurrentHashMap<String, Long>()

    val shutdownTimeout: Duration get() = settings.shutdownTimeout

    override fun publish(topic: String?, key: String?, event: Any?, headers: Map<String, String?>?) =
        accept(topic, key, headers, result = null) { serializer.serialize(event ?: throw IllegalArgumentException("publish called with a null event")) }

    override fun publishJson(topic: String?, key: String?, json: String?, headers: Map<String, String?>?) =
        accept(topic, key, headers, result = null) { json ?: throw IllegalArgumentException("publishJson called with null json") }

    override fun publishWithResult(topic: String?, key: String?, event: Any?, headers: Map<String, String?>?): CompletableFuture<PublishResult> =
        CompletableFuture<PublishResult>().also { result ->
            accept(topic, key, headers, result) { serializer.serialize(event ?: throw IllegalArgumentException("publishWithResult called with a null event")) }
        }

    /** Caller thread. Never throws: bad arguments and an Error from a serializer (a cyclic graph) are failures too. */
    private fun accept(
        topic: String?,
        key: String?,
        headers: Map<String, String?>?,
        result: CompletableFuture<PublishResult>?,
        payload: () -> String,
    ) {
        if (topic == null) {
            return failures.fail(INTERNAL, "", key, attempt = 0, cause = IllegalArgumentException("publish called with a null topic"), payload = null, result = result)
        }
        try {
            if (!open.get()) return failures.fail(CHANNEL_UNAVAILABLE, topic, key, attempt = 0, cause = null, payload = null, result = result)
            val json = try {
                payload()
            } catch (e: Throwable) {
                return failures.fail(SERIALIZATION, topic, key, attempt = 0, cause = e, payload = null, result = result)
            }
            val record = OutboundRecord(channel, topic, key, json, headerSnapshot(headers), nanoTime())
            if (!backpressure.admit(lanes.forKey(key), SendTask(record, attempt = 1, result, previousError = null))) {
                failures.fail(QUEUE_FULL, topic, key, attempt = 0, cause = null, payload = json, result = result)
            }
        } catch (e: Throwable) {
            failures.fail(INTERNAL, topic, key, attempt = 0, cause = e, payload = null, result = result)
        }
    }

    /** Lane thread (the caller under CALLER_RUNS; the maintenance thread for a retry). Never throws. */
    private fun send(record: OutboundRecord, attempt: Int, result: CompletableFuture<PublishResult>?) {
        val cooldown = cooldownFor(record.topic)
        if (cooldown is Cooldown.Active) return failCoolingDown(record, attempt, cooldown.untilNanos, result)
        val settled = AtomicBoolean(false) // one outcome per attempt, even from a sender that both calls back and throws
        var topicMissing = false // set by an outcome reported inside sender.send (the only way a missing topic is reported)
        try {
            sender.send(record) { outcome ->
                if (settled.compareAndSet(false, true)) {
                    if (outcome is SendOutcome.Failed && outcome.topicMissing) {
                        topicMissing = true
                        startCooldown(record.topic)
                    }
                    onOutcome(record, attempt, result, outcome)
                }
            }
        } catch (e: Throwable) {
            if (settled.compareAndSet(false, true)) retryOrFail(SEND_REJECTED, record, attempt, e, result)
        }
        if (cooldown is Cooldown.Probe && !topicMissing) coolingDown.remove(record.topic, cooldown.token) // the topic is back
    }

    /** Producer I/O thread, or the lane for errors the producer reports at once. Never throws. */
    private fun onOutcome(record: OutboundRecord, attempt: Int, result: CompletableFuture<PublishResult>?, outcome: SendOutcome) {
        try {
            when (outcome) {
                is SendOutcome.Delivered -> {
                    val latency = Duration.ofNanos((nanoTime() - record.enqueuedNanos).coerceAtLeast(0))
                    try {
                        metrics.delivered(channel, record.topic, latency)
                    } catch (e: Throwable) {
                        if (metricsWarnings.admit(DELIVERED)) logger.warn("kafka_publisher_metrics_failed channel={}: {}", channel, describe(e))
                    }
                    result?.complete(PublishResult(channel, record.topic, record.key, outcome.partition, outcome.offset, attempt, latency))
                }
                is SendOutcome.Failed -> retryOrFail(DELIVERY_FAILED, record, attempt, outcome.error, result)
            }
        } catch (e: Throwable) {
            failures.fail(INTERNAL, record.topic, record.key, attempt, e, record.payload, result)
        }
    }

    /** Never throws. */
    private fun retryOrFail(
        stage: PublishFailureStage,
        record: OutboundRecord,
        attempt: Int,
        error: Throwable,
        result: CompletableFuture<PublishResult>?,
    ) {
        val retriable: Boolean
        val delay: Duration?
        try {
            retriable = classifier.isRetriable(error)
            delay = if (retriable && open.get()) retryPolicy.delayBeforeAttempt(attempt + 1) else null
        } catch (e: Throwable) {
            e.addSuppressed(error) // Kotlin's addSuppressed ignores an exception suppressing itself
            return failures.fail(INTERNAL, record.topic, record.key, attempt, e, record.payload, result)
        }
        if (delay != null) {
            val retry = PendingRetry(stage, record, attempt, error, retriable, result)
            pendingRetries.add(retry)
            try {
                retry.track(maintenance.schedule(retry, delay.toMillis(), TimeUnit.MILLISECONDS))
            } catch (shuttingDown: RejectedExecutionException) {
                if (retry.claim()) retry.giveUp() // report the original failure
                return
            }
            if (!open.get()) retry.abandon() // close() swept the pending retries before this one was added
            return
        }
        failures.fail(stage, record.topic, record.key, attempt, error, record.payload, result, retriable)
    }

    // ── missing-topic cool-down (Ruling L-3) ──────────────────────────────────────────────────────────────

    private sealed interface Cooldown {
        object None : Cooldown
        class Probe(val token: Long) : Cooldown
        class Active(val untilNanos: Long) : Cooldown
    }

    /** None: send. Probe: the cool-down is over and this send is the one that checks the topic. Active: fail at once. */
    private fun cooldownFor(topic: String): Cooldown {
        val until = coolingDown[topic] ?: return Cooldown.None
        val now = nanoTime()
        if (now - until < 0) return Cooldown.Active(until)
        val token = now + cooldownNanos // while this send probes, the others keep failing fast
        if (coolingDown.replace(topic, until, token)) return Cooldown.Probe(token)
        val current = coolingDown[topic] ?: return Cooldown.None
        return if (now - current < 0) Cooldown.Active(current) else Cooldown.None
    }

    private fun startCooldown(topic: String) {
        if (cooldownNanos <= 0) return
        coolingDown[topic] = nanoTime() + cooldownNanos
        try {
            logger.warn("kafka_publisher_topic_cooling_down channel={} topic={} for={}", channel, topic, settings.producer.missingTopicCooldown)
        } catch (ignored: Throwable) {
            // the outcome must still be reported
        }
    }

    private fun failCoolingDown(record: OutboundRecord, attempt: Int, untilNanos: Long, result: CompletableFuture<PublishResult>?) {
        val until = clock.instant().plusNanos(untilNanos - nanoTime())
        failures.fail(DELIVERY_FAILED, record.topic, record.key, attempt - 1, TopicCoolingDownException(record.topic, until), record.payload, result)
    }

    /**
     * Run with the failure summaries: forgets the cool-downs that are over, so a topic that is never sent to again does
     * not keep its entry. An entry is removed only while it still holds the deadline that passed: a send that took the
     * probe meanwhile has replaced it with a later one, which stays. Once forgotten, a topic is treated like a new one.
     */
    fun expireCooldowns() {
        if (coolingDown.isEmpty()) return
        val now = nanoTime()
        for ((topic, until) in coolingDown) {
            if (now - until >= 0) coolingDown.remove(topic, until)
        }
    }

    /** The topics that have a cool-down entry, over or not. Read by tests. */
    fun coolingDownTopics(): Set<String> = coolingDown.keys.toSet()

    // ── shutdown ──────────────────────────────────────────────────────────────────────────────────────────

    /** Phase 1 of close: refuse new records, fail pending retries, stop the lanes accepting. False when already closing. */
    fun beginClose(): Boolean {
        if (!open.compareAndSet(true, false)) return false
        pendingRetries.toList().forEach { it.abandon() } // nothing is retried during shutdown
        lanes.stopAccepting()
        return true
    }

    /**
     * Phase 2: let the lanes drain until [deadlineNanos] (a `System.nanoTime()` deadline). That deadline is the cut-off:
     * the lanes are halted (no worker starts another record), whatever is still queued fails as CHANNEL_UNAVAILABLE, so
     * no future outlives close(), and is counted; only then is the producer closed with the time left. A record that was
     * queued at the deadline is therefore never sent into a closing producer (it would fail as SEND_REJECTED, uncounted).
     */
    fun finishClose(deadlineNanos: Long) {
        lanes.awaitDrained(deadlineNanos)
        val queued = lanes.haltAndTakeRemaining().filterIsInstance<LaneTask>()
        try {
            queued.forEach { it.abandon(CHANNEL_UNAVAILABLE, null) }
        } finally {
            closeProducer(deadlineNanos) // whatever happened above
        }
        lanes.awaitDrained(System.nanoTime() + IN_FLIGHT_GRACE_NANOS) // in-flight sends return once the producer is closed
        val abandoned = abandonedAtShutdown.get()
        if (abandoned > 0) logger.warn("kafka_publisher_shutdown_abandoned channel={} count={}", channel, abandoned)
    }

    private fun closeProducer(deadlineNanos: Long) {
        try {
            sender.close(Duration.ofNanos((deadlineNanos - System.nanoTime()).coerceAtLeast(0)))
        } catch (e: Throwable) {
            logger.warn("kafka_publisher_producer_close_failed channel={}: {}", channel, describe(e))
        }
    }

    /** Stop accepting, fail pending retries, drain until the shutdown timeout, give up the rest, close the producer. */
    fun close() {
        if (beginClose()) finishClose(System.nanoTime() + settings.shutdownTimeout.saturatedNanos())
    }

    /** Run with the failure summaries: the WARNs held back since the last run. */
    fun flushWarnings() {
        failures.flushWarnings()
        metricsWarnings.flush { _, count ->
            logger.warn("kafka_publisher_metrics_failed_summary channel={} count={} interval={}", channel, count, warnInterval)
        }
    }

    /** One attempt waiting in a lane: the lane sends it, or shutdown or a dead lane gives it up, once. */
    private inner class SendTask(
        private val record: OutboundRecord,
        private val attempt: Int,
        private val result: CompletableFuture<PublishResult>?,
        private val previousError: Throwable?,
    ) : LaneTask {
        override fun run() = send(record, attempt, result)

        override fun abandon(stage: PublishFailureStage, cause: Throwable?) {
            if (stage == CHANNEL_UNAVAILABLE) abandonedAtShutdown.incrementAndGet()
            failures.fail(stage, record.topic, record.key, attempt - 1, cause ?: previousError, record.payload, result)
        }
    }

    /** A scheduled retry. Exactly one of run (the delay passed) and abandon (the channel closed) acts on it. */
    private inner class PendingRetry(
        private val stage: PublishFailureStage,
        private val record: OutboundRecord,
        private val attempt: Int,
        private val error: Throwable,
        private val retriable: Boolean,
        private val result: CompletableFuture<PublishResult>?,
    ) : Runnable {
        private val claimed = AtomicBoolean(false)
        @Volatile private var scheduled: ScheduledFuture<*>? = null

        fun track(timer: ScheduledFuture<*>) {
            scheduled = timer
        }

        /** True for the one caller that may act on this retry. Either way it is no longer pending. */
        fun claim(): Boolean = claimed.compareAndSet(false, true).also { pendingRetries.remove(this) }

        /** Maintenance thread: back through the backpressure policy, at the lane's tail. */
        override fun run() {
            if (!claim()) return
            try {
                if (!open.get()) return giveUp()
                if (!backpressure.admit(lanes.forKey(record.key), SendTask(record, attempt + 1, result, previousError = error))) {
                    failures.fail(QUEUE_FULL, record.topic, record.key, attempt, error, record.payload, result, retriable)
                }
            } catch (e: Throwable) {
                failures.fail(INTERNAL, record.topic, record.key, attempt, e, record.payload, result)
            }
        }

        fun abandon() {
            if (!claim()) return
            scheduled?.cancel(false)
            giveUp()
        }

        fun giveUp() = failures.fail(stage, record.topic, record.key, attempt, error, record.payload, result, retriable)
    }

    private companion object {
        private const val DELIVERED = "delivered"

        /** After the producer closed, how long close() waits for in-flight sends to return (they do at once with Kafka). */
        private val IN_FLIGHT_GRACE_NANOS = TimeUnit.SECONDS.toNanos(1)
        private val logger = LoggerFactory.getLogger(ChannelEventPublisher::class.java)
    }
}
