package com.crp.system.libs.kafka.publisher.testing

import com.crp.system.libs.kafka.publisher.adapters.GsonEventSerializer
import com.crp.system.libs.kafka.publisher.api.EventPublisher
import com.crp.system.libs.kafka.publisher.api.EventSerializer
import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.spring.SerializerNaming
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

/** What a [RecordingEventPublisher] accepted: what the real channel would have sent. */
data class RecordedEvent(
    val channel: String,
    val topic: String,
    val key: String?,
    /** The event object; null for `publishJson`. */
    val event: Any?,
    /** The record value: the channel serializer's JSON, or the caller's JSON for `publishJson`. */
    val json: String,
    val headers: Map<String, String>,
)

/**
 * An [EventPublisher] for service tests: no broker, no threads. It serializes like the real channel
 * (snake_case Gson by default), records what would have been sent, and never throws. Thread-safe.
 */
class RecordingEventPublisher @JvmOverloads constructor(
    override val channel: String,
    naming: SerializerNaming = SerializerNaming.SNAKE_CASE,
) : EventPublisher {

    /** The serializer the real channel uses, for tests that check an event's JSON directly. */
    val serializer: EventSerializer = GsonEventSerializer(naming)

    private val events = CopyOnWriteArrayList<RecordedEvent>()
    private val rejected = CopyOnWriteArrayList<PublishFailure>()
    private val offsets = AtomicLong()

    /** The accepted events, in publish order. */
    val recorded: List<RecordedEvent> get() = events.toList()

    /** Events the real channel would have rejected on the caller thread (serialization failures). */
    val failures: List<PublishFailure> get() = rejected.toList()

    override fun publish(topic: String, key: String?, event: Any, headers: Map<String, String>) {
        record(topic, key, event, headers)
    }

    override fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String>) {
        events += RecordedEvent(channel, topic, key, null, json, headers.toMap())
    }

    override fun publishWithResult(topic: String, key: String?, event: Any, headers: Map<String, String>): CompletableFuture<PublishResult> {
        val failure = record(topic, key, event, headers)
            ?: return CompletableFuture.completedFuture(PublishResult(channel, topic, key, 0, offsets.getAndIncrement(), 1, Duration.ZERO))
        return CompletableFuture.failedFuture(PublishFailedException(failure))
    }

    fun clear() {
        events.clear()
        rejected.clear()
    }

    private fun record(topic: String, key: String?, event: Any, headers: Map<String, String>): PublishFailure? =
        try {
            events += RecordedEvent(channel, topic, key, event, serializer.serialize(event), headers.toMap())
            null
        } catch (e: Throwable) {
            PublishFailure(channel, topic, key, PublishFailureStage.SERIALIZATION, 0, false, e, null, Instant.now()).also { rejected += it }
        }
}
