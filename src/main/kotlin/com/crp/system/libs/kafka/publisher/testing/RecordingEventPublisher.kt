package com.crp.system.libs.kafka.publisher.testing

import com.crp.system.libs.kafka.publisher.adapters.GsonEventSerializer
import com.crp.system.libs.kafka.publisher.api.EventPublisher
import com.crp.system.libs.kafka.publisher.api.EventSerializer
import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.core.headerSnapshot
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
    /** The headers as sent: without null names or values. */
    val headers: Map<String, String>,
)

/**
 * An [EventPublisher] for service tests: no broker, no threads. It serializes like the real channel
 * (snake_case Gson by default), records what would have been sent, and never throws: what the real channel would
 * reject on the caller thread (a null topic, a null or unserializable event) goes to [failures] instead. Thread-safe.
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

    /** What the real channel would have rejected on the caller thread: a null topic (INTERNAL), a bad event (SERIALIZATION). */
    val failures: List<PublishFailure> get() = rejected.toList()

    override fun publish(topic: String?, key: String?, event: Any?, headers: Map<String, String?>?) {
        accept(topic, key, event, headers) { serializer.serialize(event ?: throw IllegalArgumentException("publish called with a null event")) }
    }

    override fun publishJson(topic: String?, key: String?, json: String?, headers: Map<String, String?>?) {
        accept(topic, key, null, headers) { json ?: throw IllegalArgumentException("publishJson called with null json") }
    }

    override fun publishWithResult(topic: String?, key: String?, event: Any?, headers: Map<String, String?>?): CompletableFuture<PublishResult> =
        when (val outcome = accept(topic, key, event, headers) { serializer.serialize(event ?: throw IllegalArgumentException("publishWithResult called with a null event")) }) {
            is PublishFailure -> CompletableFuture.failedFuture(PublishFailedException(outcome))
            else -> CompletableFuture.completedFuture(PublishResult(channel, topic.orEmpty(), key, 0, offsets.getAndIncrement(), 1, Duration.ZERO))
        }

    fun clear() {
        events.clear()
        rejected.clear()
    }

    /** Records the event, or the failure the real channel would report; returns the failure, or null when recorded. */
    private fun accept(topic: String?, key: String?, event: Any?, headers: Map<String, String?>?, json: () -> String): PublishFailure? {
        if (topic == null) return reject(PublishFailureStage.INTERNAL, "", key, IllegalArgumentException("publish called with a null topic"))
        return try {
            events += RecordedEvent(channel, topic, key, event, json(), headerSnapshot(headers))
            null
        } catch (e: Throwable) {
            reject(PublishFailureStage.SERIALIZATION, topic, key, e)
        }
    }

    private fun reject(stage: PublishFailureStage, topic: String, key: String?, cause: Throwable): PublishFailure =
        PublishFailure(channel, topic, key, stage, 0, false, cause, null, Instant.now()).also { rejected += it }
}
