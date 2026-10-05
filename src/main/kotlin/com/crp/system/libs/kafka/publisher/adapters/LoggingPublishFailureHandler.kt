package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

/**
 * Logs the first failure per (channel, topic, stage) in each summary interval and counts the rest.
 * Drops (QUEUE_FULL) are only counted. CHANNEL_UNAVAILABLE is not logged. Never logs payloads (player data).
 */
internal class LoggingPublishFailureHandler(private val summaryInterval: Duration, private val clock: Clock) : PublishFailureHandler {
    private data class SummaryKey(val channel: String, val topic: String, val stage: PublishFailureStage)

    private val lastLoggedAt = ConcurrentHashMap<SummaryKey, AtomicLong>()
    private val suppressed = ConcurrentHashMap<SummaryKey, LongAdder>()

    override fun onFailure(failure: PublishFailure) {
        if (failure.stage == CHANNEL_UNAVAILABLE) return
        val key = SummaryKey(failure.channel, failure.topic, failure.stage)
        val now = clock.millis()
        val last = lastLoggedAt.computeIfAbsent(key) { AtomicLong(NEVER) }
        val previous = last.get()
        val due = previous == NEVER || now - previous >= summaryInterval.toMillis()
        if (failure.stage != QUEUE_FULL && due && last.compareAndSet(previous, now)) {
            logger.error(
                "kafka_publish_failed channel={} topic={} key={} stage={} attempt={} retriable={} cause={}",
                failure.channel, failure.topic, failure.key, failure.stage, failure.attempt, failure.retriable, causeChain(failure.cause),
            )
        } else {
            suppressed.computeIfAbsent(key) { LongAdder() }.increment()
        }
    }

    /** Run every summary interval by the maintenance thread, and once at shutdown. */
    fun flushSummaries() = suppressed.forEach { (key, counter) ->
        val count = counter.sumThenReset()
        if (count > 0) {
            logger.warn(
                "kafka_publish_failed_summary channel={} topic={} stage={} count={} interval={}",
                key.channel, key.topic, key.stage, count, summaryInterval,
            )
        }
    }

    private fun causeChain(error: Throwable?) =
        generateSequence(error) { it.cause }.take(5).joinToString(" <- ") { "${it.javaClass.simpleName}: ${it.message}" }

    private companion object {
        private const val NEVER = Long.MIN_VALUE
        private val logger = LoggerFactory.getLogger(LoggingPublishFailureHandler::class.java)
    }
}
