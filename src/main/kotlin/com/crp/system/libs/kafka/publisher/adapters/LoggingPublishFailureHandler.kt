package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.core.RateLimiter
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Logs the first failure per (channel, topic, stage) in each summary interval and counts the rest.
 * Drops (QUEUE_FULL) are only counted. CHANNEL_UNAVAILABLE is not logged. Never logs payloads (player data).
 * Monotonic time, so a wall-clock step can neither silence an ERROR nor repeat it.
 */
internal class LoggingPublishFailureHandler(
    private val summaryInterval: Duration,
    nanoTime: () -> Long,
) : PublishFailureHandler {
    private data class SummaryKey(val channel: String, val topic: String, val stage: PublishFailureStage)

    private val limiter = RateLimiter<SummaryKey>(summaryInterval, nanoTime)

    override fun onFailure(failure: PublishFailure) {
        if (failure.stage == CHANNEL_UNAVAILABLE) return
        val key = SummaryKey(failure.channel, failure.topic, failure.stage)
        if (failure.stage == QUEUE_FULL) return limiter.hold(key)
        if (limiter.admit(key)) {
            logger.error(
                "kafka_publish_failed channel={} topic={} key={} stage={} attempt={} retriable={} cause={}",
                failure.channel, failure.topic, failure.key, failure.stage, failure.attempt, failure.retriable, causeChain(failure.cause),
            )
        }
    }

    /** Run every summary interval by the maintenance thread, and once at shutdown. */
    fun flushSummaries() = limiter.flush { key, count ->
        logger.warn(
            "kafka_publish_failed_summary channel={} topic={} stage={} count={} interval={}",
            key.channel, key.topic, key.stage, count, summaryInterval,
        )
    }

    private fun causeChain(error: Throwable?) =
        generateSequence(error) { it.cause }.take(5).joinToString(" <- ") { "${it.javaClass.simpleName}: ${it.message}" }

    private companion object {
        private val logger = LoggerFactory.getLogger(LoggingPublishFailureHandler::class.java)
    }
}
