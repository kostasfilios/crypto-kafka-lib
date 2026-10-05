package com.crp.system.libs.kafka.publisher.api

import java.util.concurrent.CompletableFuture

/**
 * The only thing business code calls. One instance per channel (`crypto.kafka.publisher.channels.<name>`).
 *
 * - **Never throws.** Every failure goes to the channel's failure handlers (and to the `publishWithResult` future).
 * - **Returns at once.** The event is serialized on the caller thread, then queued; the send runs on a lane thread.
 *   Two policies are the bounded exceptions: CALLER_RUNS (a full queue makes the caller send) and
 *   BLOCK_WITH_TIMEOUT (a full queue makes the caller wait up to `block-timeout`).
 * - **Best effort.** Events still queued when the process dies are lost. Must-deliver events need an outbox.
 */
interface EventPublisher {
    val channel: String

    /** Serializes [event] with the channel's serializer (snake_case Gson by default) and sends it keyed by [key]. */
    fun publish(topic: String, key: String?, event: Any, headers: Map<String, String> = emptyMap())

    /** Sends [json] as it is (for topics that keep their own JSON style). */
    fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String> = emptyMap())

    /**
     * Like [publish], and tells the caller the outcome: the future completes with a [PublishResult] on the broker's
     * acknowledgement, or exceptionally with a [PublishFailedException]. It usually completes on the Kafka producer's
     * I/O thread: continuations must be quick, or use the `...Async(executor)` variants.
     */
    fun publishWithResult(
        topic: String,
        key: String?,
        event: Any,
        headers: Map<String, String> = emptyMap(),
    ): CompletableFuture<PublishResult>
}

/** The catalog of configured channels. */
interface EventPublisherChannels {
    /** An unknown or disabled channel returns a publisher that does nothing; an unknown name also logs one WARN. */
    fun get(name: String): EventPublisher

    fun names(): Set<String>
}
