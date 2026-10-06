package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture

/** Every failure goes through here exactly once: metrics, then each handler, then the caller's future. Never throws. */
internal class FailureDispatcher(
    private val channel: String,
    private val handlers: List<PublishFailureHandler>,
    private val metrics: PublisherMetrics,
    private val clock: Clock,
    private val warnInterval: Duration,
    nanoTime: () -> Long,
) {
    /** A handler that keeps failing logs one WARN per interval; the rest go into [flushWarnings]' summary. */
    private val warnings = RateLimiter<String>(warnInterval, nanoTime)

    fun fail(
        stage: PublishFailureStage,
        topic: String,
        key: String?,
        attempt: Int,
        cause: Throwable?,
        payload: String?,
        result: CompletableFuture<PublishResult>?,
        retriable: Boolean = false,
    ) {
        val failure = PublishFailure(channel, topic, key, stage, attempt, retriable, cause, payload, clock.instant())
        try {
            metrics.failed(channel, topic, stage)
        } catch (e: Throwable) {
            warnFailed("metrics", e)
        }
        for (handler in handlers) {
            try {
                handler.onFailure(failure)
            } catch (e: Throwable) {
                warnFailed(nameOf(handler), e)
            }
        }
        result?.completeExceptionally(PublishFailedException(failure))
    }

    /** Logs, then resets, how many handler WARNs were held back since the last flush. */
    fun flushWarnings() = warnings.flush { handler, count ->
        logger.warn("kafka_publisher_failure_handler_failed_summary channel={} handler={} count={} interval={}", channel, handler, count, warnInterval)
    }

    private fun warnFailed(handler: String, error: Throwable) {
        if (warnings.admit(handler)) logger.warn("kafka_publisher_failure_handler_failed channel={} handler={}: {}", channel, handler, describe(error))
    }

    /** An anonymous handler has no simple name: fall back to the class name. */
    private fun nameOf(handler: PublishFailureHandler): String = handler.javaClass.simpleName.ifEmpty { handler.javaClass.name }

    private companion object {
        private val logger = LoggerFactory.getLogger(FailureDispatcher::class.java)
    }
}
