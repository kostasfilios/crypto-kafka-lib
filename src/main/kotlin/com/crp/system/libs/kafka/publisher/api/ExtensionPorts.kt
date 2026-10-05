package com.crp.system.libs.kafka.publisher.api

import java.time.Duration

/** Turns an event into the record's JSON value. Runs on the caller thread. */
fun interface EventSerializer {
    fun serialize(event: Any): String
}

/**
 * The end of the exception-handling layer. Every channel calls the built-in handlers, then each
 * `PublishFailureHandler` bean of the service.
 *
 * May run on the Kafka producer's I/O thread: it must not block.
 */
fun interface PublishFailureHandler {
    fun onFailure(failure: PublishFailure)
}

/** Decides whether a send error is worth another attempt. */
fun interface PublishFailureClassifier {
    fun isRetriable(error: Throwable): Boolean
}

/** Returns the delay before [nextAttempt], or null to stop retrying. */
fun interface RetryPolicy {
    fun delayBeforeAttempt(nextAttempt: Int): Duration?
}

/** Publisher metrics. Micrometer when the service has a `MeterRegistry` bean, [None] otherwise. */
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
