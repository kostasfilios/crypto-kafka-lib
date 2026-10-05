package com.crp.system.libs.kafka.publisher.api

import java.time.Duration
import java.time.Instant

/** A record the broker acknowledged. */
data class PublishResult(
    val channel: String,
    val topic: String,
    val key: String?,
    val partition: Int,
    val offset: Long,
    val attempts: Int,
    /** From enqueue to the broker's acknowledgement, on the monotonic clock. */
    val latency: Duration,
)

/** Where a publish failed. */
enum class PublishFailureStage {
    /** Caller thread: the event could not be turned into JSON (a null event or json included). */
    SERIALIZATION,

    /** Caller thread: the lane's queue was full (DROP, or BLOCK_WITH_TIMEOUT after its wait). */
    QUEUE_FULL,

    /** Lane thread: `producer.send` threw (producer closed, interrupted, a non-API KafkaException). */
    SEND_REJECTED,

    /**
     * Producer callback: topic not in metadata after `max-block-ms`, buffer full, record too large,
     * batch expired after `delivery-timeout-ms`, not enough replicas, authorization.
     * Also, without a send, a topic cooling down after it was missing from the metadata ([TopicCoolingDownException]).
     */
    DELIVERY_FAILED,

    /** Caller thread: channel disabled, unknown or shutting down. Also a record still queued when shutdown gave up. */
    CHANNEL_UNAVAILABLE,

    /** Any thread: a defect inside the publisher, a lane whose worker died twice, or a null topic. */
    INTERNAL,
}

/** What failure handlers receive. */
data class PublishFailure(
    val channel: String,
    val topic: String,
    val key: String?,
    val stage: PublishFailureStage,
    /** 0 = the record never reached Kafka. */
    val attempt: Int,
    val retriable: Boolean,
    val cause: Throwable?,
    /** The JSON when it exists. The logging handler never logs it (player data), and [toString] only shows its length. */
    val payload: String?,
    val occurredAt: Instant,
) {
    override fun toString(): String =
        "PublishFailure(channel=$channel, topic=$topic, key=$key, stage=$stage, attempt=$attempt, retriable=$retriable, " +
            "cause=$cause, payload=${payload?.let { "<${it.length} chars>" }}, occurredAt=$occurredAt)"
}

/** How a `publishWithResult` future fails. */
class PublishFailedException(val failure: PublishFailure) :
    RuntimeException("publish failed: channel=${failure.channel} topic=${failure.topic} stage=${failure.stage}", failure.cause)

/**
 * The cause of a DELIVERY_FAILED record that was not sent because its topic is cooling down: the producer could not
 * find the topic in the broker's metadata within `max-block-ms`, so sends to it fail at once until [until], then one
 * send checks the topic again. No stack trace: one is created per record while the topic cools down.
 */
class TopicCoolingDownException(val topic: String, val until: Instant) :
    RuntimeException("topic $topic was missing from the broker's metadata; sends to it fail at once until $until", null, false, false)
