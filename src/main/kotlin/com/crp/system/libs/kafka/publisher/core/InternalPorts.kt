package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import java.time.Duration

/** A record ready to send: the JSON was produced on the caller thread (a snapshot of the event). */
internal data class OutboundRecord(
    val channel: String,
    val topic: String,
    val key: String?,
    val payload: String,
    val headers: Map<String, String>,
    /** Monotonic enqueue time (`System.nanoTime()` unless a test injects a source), so latency is never negative. */
    val enqueuedNanos: Long,
)

internal sealed interface SendOutcome {
    data class Delivered(val partition: Int, val offset: Long) : SendOutcome

    /** [topicMissing]: the sender could not find the topic in the broker's metadata; it starts the topic's cool-down. */
    data class Failed(val error: Exception, val topicMissing: Boolean = false) : SendOutcome
}

/** `send` may throw (-> SEND_REJECTED); otherwise `onOutcome` is called exactly once. */
internal interface RecordSender {
    fun send(record: OutboundRecord, onOutcome: (SendOutcome) -> Unit)
    fun close(timeout: Duration)
}

/** Which of [topics] the broker does not have. Throws when it cannot tell. */
internal fun interface TopicInspector {
    fun missingTopics(topics: Collection<String>, timeout: Duration): Set<String>
}

/** Admits a send task to a lane; false = the record was not taken (QUEUE_FULL). */
internal fun interface BackpressurePolicy {
    fun admit(lane: PublishLane, task: Runnable): Boolean
}

/** A queued send. The lane runs it, or gives it up (at shutdown, or when its worker died) so its future is never stranded. */
internal interface LaneTask : Runnable {
    fun abandon(stage: PublishFailureStage, cause: Throwable?)
}

/** The headers to send: a snapshot of the caller's map without null names or values. */
internal fun headerSnapshot(headers: Map<String, String?>?): Map<String, String> {
    if (headers.isNullOrEmpty()) return emptyMap()
    val copy = LinkedHashMap<String, String>()
    for (entry in headers.entries) {
        val name: String? = entry.key // a Java caller can pass a null name
        val value = entry.value
        if (name != null && value != null) copy[name] = value
    }
    return copy
}

/** Nanoseconds of [this], saturated instead of overflowing for absurd durations. */
internal fun Duration.saturatedNanos(): Long =
    try {
        toNanos()
    } catch (e: ArithmeticException) {
        if (isNegative) Long.MIN_VALUE / 4 else Long.MAX_VALUE / 4
    }

/** `toString()` that cannot throw. */
internal fun describe(error: Throwable): String =
    try {
        error.toString()
    } catch (e: Throwable) {
        error.javaClass.name
    }
