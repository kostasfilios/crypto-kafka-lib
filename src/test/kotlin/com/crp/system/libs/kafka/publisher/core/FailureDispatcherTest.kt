package com.crp.system.libs.kafka.publisher.core

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import com.crp.system.libs.kafka.publisher.testsupport.RecordingFailureHandler
import com.crp.system.libs.kafka.publisher.testsupport.RecordingMetrics
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import org.apache.kafka.common.errors.TimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList

class FailureDispatcherTest {
    private val clock = MutableClock()
    private val cause = TimeoutException("not present in metadata after 2000 ms")
    private val interval = Duration.ofSeconds(10)

    @Test
    fun `builds the failure, then calls metrics, every handler in order, then completes the future`() {
        val order = CopyOnWriteArrayList<String>()
        val result = CompletableFuture<PublishResult>()
        val metrics = object : PublisherMetrics by PublisherMetrics.None {
            override fun failed(channel: String, topic: String, stage: PublishFailureStage) {
                order += "metrics:$channel:$topic:$stage"
            }
        }
        val first = PublishFailureHandler { order += "first:done=${result.isDone}" }
        val second = PublishFailureHandler { order += "second:done=${result.isDone}" }

        FailureDispatcher("reporting", listOf(first, second), metrics, clock, interval, clock::nanoTime)
            .fail(DELIVERY_FAILED, "t1", "42", attempt = 2, cause = cause, payload = "{}", result = result, retriable = true)

        assertThat(order).containsExactly("metrics:reporting:t1:DELIVERY_FAILED", "first:done=false", "second:done=false")
        assertThat(result.awaitFailure()).isEqualTo(
            PublishFailure("reporting", "t1", "42", DELIVERY_FAILED, 2, true, cause, "{}", clock.instant()),
        )
    }

    @Test
    fun `a throwing handler does not break the chain, and the future still completes`() {
        val recorder = RecordingFailureHandler()
        val result = CompletableFuture<PublishResult>()
        LogCapture(FailureDispatcher::class.java).use { logs ->
            val throwing = PublishFailureHandler { throw IllegalStateException("handler down") }
            val throwingError = PublishFailureHandler { throw NoClassDefFoundError("missing") }

            assertDoesNotThrow {
                FailureDispatcher("reporting", listOf(throwing, throwingError, recorder), RecordingMetrics(), clock, interval, clock::nanoTime)
                    .fail(DELIVERY_FAILED, "t1", "42", 1, cause, "{}", result)
            }

            assertThat(recorder.failures).hasSize(1)
            assertThat(result.awaitFailure().stage).isEqualTo(DELIVERY_FAILED)
            assertThat(logs.lines(Level.WARN)).hasSize(2).allMatch { it.startsWith("kafka_publisher_failure_handler_failed channel=reporting handler=") }
            assertThat(logs.lines(Level.WARN).first()).contains("IllegalStateException: handler down")
        }
    }

    @Test
    fun `a handler that keeps failing is warned once per interval, and the rest are summarised`() {
        LogCapture(FailureDispatcher::class.java).use { logs ->
            val dispatcher = FailureDispatcher(
                "reporting", listOf(NamedThrowingHandler()), RecordingMetrics(), clock, Duration.ofSeconds(10), clock::nanoTime,
            )
            repeat(3) { dispatcher.fail(DELIVERY_FAILED, "t1", "k", 1, cause, null, null) }
            assertThat(logs.lines(Level.WARN)).containsExactly(
                "kafka_publisher_failure_handler_failed channel=reporting handler=NamedThrowingHandler: java.lang.IllegalStateException: handler down",
            )

            dispatcher.flushWarnings()
            assertThat(logs.lines(Level.WARN).last())
                .isEqualTo("kafka_publisher_failure_handler_failed_summary channel=reporting handler=NamedThrowingHandler count=2 interval=PT10S")

            clock.advance(Duration.ofSeconds(10))
            dispatcher.fail(DELIVERY_FAILED, "t1", "k", 1, cause, null, null)
            assertThat(logs.lines(Level.WARN, "kafka_publisher_failure_handler_failed channel=")).hasSize(2)
        }
    }

    @Test
    fun `an anonymous handler is named by its class name`() {
        LogCapture(FailureDispatcher::class.java).use { logs ->
            val anonymous = object : PublishFailureHandler {
                override fun onFailure(failure: PublishFailure) = throw IllegalStateException("anonymous down")
            }
            assertThat(anonymous.javaClass.simpleName).isEmpty()
            FailureDispatcher("reporting", listOf(anonymous), RecordingMetrics(), clock, interval, clock::nanoTime).fail(DELIVERY_FAILED, "t1", "k", 1, cause, null, null)
            assertThat(logs.lines(Level.WARN).single()).contains("handler=${anonymous.javaClass.name}:")
        }
    }

    private class NamedThrowingHandler : PublishFailureHandler {
        override fun onFailure(failure: PublishFailure) = throw IllegalStateException("handler down")
    }

    @Test
    fun `throwing metrics do not stop the handlers, and are warned about`() {
        val recorder = RecordingFailureHandler()
        val metrics = object : PublisherMetrics by PublisherMetrics.None {
            override fun failed(channel: String, topic: String, stage: PublishFailureStage) = throw IllegalStateException("metrics down")
        }
        LogCapture(FailureDispatcher::class.java).use { logs ->
            FailureDispatcher("reporting", listOf(recorder), metrics, clock, interval, clock::nanoTime).fail(DELIVERY_FAILED, "t1", null, 1, cause, null, null)
            assertThat(recorder.failures).hasSize(1)
            assertThat(logs.lines(Level.WARN)).containsExactly(
                "kafka_publisher_failure_handler_failed channel=reporting handler=metrics: java.lang.IllegalStateException: metrics down",
            )
        }
    }

    @Test
    fun `the future completes exactly once with the first failure`() {
        val result = CompletableFuture<PublishResult>()
        val dispatcher = FailureDispatcher("reporting", emptyList(), RecordingMetrics(), clock, interval, clock::nanoTime)
        dispatcher.fail(DELIVERY_FAILED, "t1", "k", 1, cause, "{}", result)
        dispatcher.fail(PublishFailureStage.INTERNAL, "t1", "k", 1, IllegalStateException(), null, result)

        assertThat(result.awaitFailure().stage).isEqualTo(DELIVERY_FAILED)
        assertThat(result.isCompletedExceptionally).isTrue()
    }

    @Test
    fun `the exception names the channel, topic and stage and keeps the cause`() {
        val result = CompletableFuture<PublishResult>()
        FailureDispatcher("reporting", emptyList(), RecordingMetrics(), clock, interval, clock::nanoTime).fail(DELIVERY_FAILED, "t1", "k", 1, cause, "{}", result)
        val error = result.handle { _, e -> e }.get()
        assertThat(error).isInstanceOf(PublishFailedException::class.java)
        assertThat(error).hasMessage("publish failed: channel=reporting topic=t1 stage=DELIVERY_FAILED")
        assertThat(error.cause).isSameAs(cause)
    }

    @Test
    fun `retriable defaults to false and the time comes from the clock`() {
        val recorder = RecordingFailureHandler()
        clock.advance(Duration.ofMinutes(3))
        FailureDispatcher("reporting", listOf(recorder), RecordingMetrics(), clock, interval, clock::nanoTime).fail(DELIVERY_FAILED, "t1", "k", 1, cause, null, null)
        assertThat(recorder.failures.single().retriable).isFalse()
        assertThat(recorder.failures.single().occurredAt).isEqualTo(clock.instant())
    }
}
