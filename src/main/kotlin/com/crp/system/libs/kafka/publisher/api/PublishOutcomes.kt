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
    /** From enqueue to the broker's acknowledgement. */
    val latency: Duration,
)

/** Where a publish failed. */
enum class PublishFailureStage {
    /** Caller thread: the event could not be turned into JSON. */
    SERIALIZATION,

    /** Caller thread: the lane's queue was full (DROP, or BLOCK_WITH_TIMEOUT after its wait). */
    QUEUE_FULL,

    /** Lane thread: `producer.send` threw (producer closed, interrupted, a non-API KafkaException). */
    SEND_REJECTED,

    /**
     * Producer callback: topic not in metadata after `max-block-ms`, buffer full, record too large,
     * batch expired after `delivery-timeout-ms`, not enough replicas, authorization.
     */
    DELIVERY_FAILED,

    /** Caller thread: channel disabled, unknown or shutting down. */
    CHANNEL_UNAVAILABLE,

    /** Any thread: a defect inside the publisher itself. */
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
    /** The JSON when it exists. The logging handler never logs it (player data). */
    val payload: String?,
    val occurredAt: Instant,
)

/** How a `publishWithResult` future fails. */
class PublishFailedException(val failure: PublishFailure) :
    RuntimeException("publish failed: channel=${failure.channel} topic=${failure.topic} stage=${failure.stage}", failure.cause)
