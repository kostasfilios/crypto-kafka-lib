package com.crp.system.libs.kafka.publisher.core

import java.time.Duration
import java.time.Instant

/** A record ready to send: the JSON was produced on the caller thread (a snapshot of the event). */
internal data class OutboundRecord(
    val channel: String,
    val topic: String,
    val key: String?,
    val payload: String,
    val headers: Map<String, String>,
    val enqueuedAt: Instant,
)

internal sealed interface SendOutcome {
    data class Delivered(val partition: Int, val offset: Long) : SendOutcome
    data class Failed(val error: Exception) : SendOutcome
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
