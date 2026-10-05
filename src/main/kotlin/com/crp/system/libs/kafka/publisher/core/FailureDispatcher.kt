package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import org.slf4j.LoggerFactory
import java.time.Clock
import java.util.concurrent.CompletableFuture

/** Every failure goes through here exactly once: metrics, then each handler, then the caller's future. Never throws. */
internal class FailureDispatcher(
    private val channel: String,
    private val handlers: List<PublishFailureHandler>,
    private val metrics: PublisherMetrics,
    private val clock: Clock,
) {
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
        guarded("metrics") { metrics.failed(channel, topic, stage) }
        for (handler in handlers) guarded(handler.javaClass.simpleName) { handler.onFailure(failure) }
        result?.completeExceptionally(PublishFailedException(failure))
    }

    private inline fun guarded(what: String, block: () -> Unit) {
        try {
            block()
        } catch (e: Throwable) {
            logger.warn("kafka_publisher_failure_handler_failed channel={} handler={}: {}", channel, what, e.toString())
        }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(FailureDispatcher::class.java)
    }
}
