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
) : EventPublisher {

    private val open = AtomicBoolean(true)
    private val pendingRetries: MutableSet<PendingRetry> = ConcurrentHashMap.newKeySet()

    override fun publish(topic: String, key: String?, event: Any, headers: Map<String, String>) =
        accept(topic, key, headers, result = null) { serializer.serialize(event) }

    override fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String>) =
        accept(topic, key, headers, result = null) { json }

    override fun publishWithResult(topic: String, key: String?, event: Any, headers: Map<String, String>): CompletableFuture<PublishResult> =
        CompletableFuture<PublishResult>().also { result -> accept(topic, key, headers, result) { serializer.serialize(event) } }

    /** Caller thread. Never throws: an Error from a serializer (a cyclic graph's StackOverflowError) is a failure too. */
    private fun accept(
        topic: String,
        key: String?,
        headers: Map<String, String>,
        result: CompletableFuture<PublishResult>?,
        payload: () -> String,
    ) {
        try {
            if (!open.get()) return failures.fail(CHANNEL_UNAVAILABLE, topic, key, attempt = 0, cause = null, payload = null, result = result)
            val json = try {
                payload()
            } catch (e: Throwable) {
                return failures.fail(SERIALIZATION, topic, key, attempt = 0, cause = e, payload = null, result = result)
            }
            val record = OutboundRecord(channel, topic, key, json, headers.toMap(), clock.instant())
            if (!backpressure.admit(lanes.forKey(key)) { send(record, attempt = 1, result) }) {
                failures.fail(QUEUE_FULL, topic, key, attempt = 0, cause = null, payload = json, result = result)
            }
        } catch (e: Throwable) {
            failures.fail(INTERNAL, topic, key, attempt = 0, cause = e, payload = null, result = result)
        }
    }

    /** Lane thread (the caller under CALLER_RUNS; the maintenance thread for a retry). Never throws. */
    private fun send(record: OutboundRecord, attempt: Int, result: CompletableFuture<PublishResult>?) {
        val settled = AtomicBoolean(false) // one outcome per attempt, even from a sender that both calls back and throws
        try {
            sender.send(record) { outcome -> if (settled.compareAndSet(false, true)) onOutcome(record, attempt, result, outcome) }
        } catch (e: Throwable) {
            if (settled.compareAndSet(false, true)) retryOrFail(SEND_REJECTED, record, attempt, e, result)
        }
    }

    /** Producer I/O thread, or the lane for errors the producer reports at once. Never throws. */
    private fun onOutcome(record: OutboundRecord, attempt: Int, result: CompletableFuture<PublishResult>?, outcome: SendOutcome) {
        try {
            when (outcome) {
                is SendOutcome.Delivered -> {
                    val latency = Duration.between(record.enqueuedAt, clock.instant())
                    try {
                        metrics.delivered(channel, record.topic, latency)
                    } catch (e: Throwable) {
                        logger.warn("kafka_publisher_metrics_failed channel={}: {}", channel, e.toString())
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

    /** Stop accepting, fail pending retries, let the lanes drain until the deadline, then close the producer with the time left. */
    fun close() {
        if (!open.compareAndSet(true, false)) return
        val deadlineNanos = System.nanoTime() + settings.shutdownTimeout.toNanos()
        pendingRetries.toList().forEach { it.abandon() } // nothing is retried during shutdown
        lanes.drainAndStop(deadlineNanos)
        sender.close(Duration.ofNanos((deadlineNanos - System.nanoTime()).coerceAtLeast(0)))
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
                if (!backpressure.admit(lanes.forKey(record.key)) { send(record, attempt + 1, result) }) {
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
        private val logger = LoggerFactory.getLogger(ChannelEventPublisher::class.java)
    }
}
