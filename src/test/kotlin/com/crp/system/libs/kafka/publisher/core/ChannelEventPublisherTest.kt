package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.EventSerializer
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.INTERNAL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.SEND_REJECTED
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.SERIALIZATION
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.spring.BackpressureMode
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.apache.kafka.common.errors.RecordTooLargeException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ChannelEventPublisherTest {
    private val harnesses = mutableListOf<Harness>()

    @AfterEach
    fun closeAll() = harnesses.forEach { it.close() }

    private fun harness(
        settings: ChannelSettings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 16),
        sender: FakeRecordSender = FakeRecordSender(),
        serializer: EventSerializer? = null,
        backpressure: BackpressurePolicy? = null,
        extraHandlers: List<PublishFailureHandler> = emptyList(),
    ): Harness = (
        if (serializer == null) Harness(settings, sender, backpressure = backpressure, extraHandlers = extraHandlers)
        else Harness(settings, sender, serializer = serializer, backpressure = backpressure, extraHandlers = extraHandlers)
        ).also { harnesses += it }

    private val event = SampleEvent("player_registered", "-7343247560384022447")
    private val json = """{"event_type":"player_registered","player_id":"-7343247560384022447","seq":0}"""

    // ── delivery and the result future ────────────────────────────────────────────────────────────────

    @Test
    fun `delivered record completes the future with partition, offset, attempts and latency from enqueue`() {
        val sender = FakeRecordSender()
        val h = harness(sender = sender)
        sender.script = { _, _, onOutcome ->
            h.clock.advance(Duration.ofMillis(25))
            onOutcome(SendOutcome.Delivered(partition = 2, offset = 41))
        }

        val result = h.publisher.publishWithResult("t1", "42", event).awaitResult()

        assertThat(result).isEqualTo(PublishResult(h.channel, "t1", "42", 2, 41, attempts = 1, latency = Duration.ofMillis(25)))
        assertThat(h.metrics.delivered).containsExactly(Triple(h.channel, "t1", Duration.ofMillis(25)))
        assertThat(h.handler.failures).isEmpty()
        assertThat(h.metrics.failed).isEmpty()
    }

    @Test
    fun `the record carries the channel, topic, key, snake_case JSON, headers and the monotonic enqueue time`() {
        val h = harness()
        val enqueuedNanos = h.clock.nanoTime()
        h.publisher.publishWithResult("t1", "42", event, mapOf("source_service" to "AccountServices")).awaitResult()

        val record = h.sender.calls.single().record
        assertThat(record).isEqualTo(OutboundRecord(h.channel, "t1", "42", json, mapOf("source_service" to "AccountServices"), enqueuedNanos))
    }

    @Test
    fun `publishJson sends the caller's JSON unchanged`() {
        val h = harness()
        h.publisher.publishJson("user-kyc-status-updates", "42", """{"userId":42}""")
        assertThat(h.sender.nextCall().record.payload).isEqualTo("""{"userId":42}""")
    }

    @Test
    fun `the event is serialized on the caller thread, and later changes to the headers map do not leak into the record`() {
        val serializedOn = CompletableFuture<String>()
        val h = harness(serializer = EventSerializer { e -> serializedOn.complete(Thread.currentThread().name); e.toString() })
        val headers = mutableMapOf("a" to "1")

        h.publisher.publish("t1", "k", event, headers)
        headers["a"] = "changed"
        headers["b"] = "added"

        assertThat(serializedOn.awaitResult()).isEqualTo(Thread.currentThread().name)
        val call = h.sender.nextCall()
        assertThat(call.record.headers).isEqualTo(mapOf("a" to "1"))
        assertThat(call.thread).isEqualTo("${h.channel}-publisher-0")
    }

    @Test
    fun `latency is measured on the monotonic clock, so a wall clock stepping back cannot make it negative`() {
        val sender = FakeRecordSender()
        val h = harness(sender = sender)
        sender.script = { _, _, onOutcome ->
            h.clock.stepWallClock(Duration.ofHours(-1))
            h.clock.advance(Duration.ofMillis(5))
            onOutcome(SendOutcome.Delivered(0, 1))
        }
        assertThat(h.publisher.publishWithResult("t1", "k", event).awaitResult().latency).isEqualTo(Duration.ofMillis(5))
    }

    // null arguments (Ruling L-1, C9): reported, never thrown

    @Test
    fun `a null topic is an INTERNAL failure with a clear message, and nothing is sent`() {
        val h = harness()
        val failure = h.publisher.publishWithResult(null, "k", event).awaitFailure()
        assertThat(failure.stage).isEqualTo(INTERNAL)
        assertThat(failure.topic).isEmpty()
        assertThat(failure.cause).isInstanceOf(IllegalArgumentException::class.java).hasMessage("publish called with a null topic")
        assertDoesNotThrow { h.publisher.publish(null, null, null, null) }
        assertDoesNotThrow { h.publisher.publishJson(null, null, null, null) }
        assertThat(h.sender.calls).isEmpty()
        h.publisher.close()
        assertThat(h.publisher.publishWithResult(null, "k", event).awaitFailure().stage).isEqualTo(INTERNAL) // closed or not
    }

    @Test
    fun `a null event or json is a SERIALIZATION failure`() {
        val h = harness()
        h.publisher.publishWithResult("t1", "k", null)
        h.publisher.publish("t1", "k", null)
        h.publisher.publishJson("t1", "k", null)
        assertThat(h.handler.awaitFailures(3).map { it.stage to it.cause?.message }).containsExactly(
            SERIALIZATION to "publishWithResult called with a null event",
            SERIALIZATION to "publish called with a null event",
            SERIALIZATION to "publishJson called with null json",
        )
        assertThat(h.sender.calls).isEmpty()
    }

    @Test
    fun `null headers send none, and a header with a null name or value is left out`() {
        val h = harness()
        h.publisher.publishWithResult("t1", "k", event, null).awaitResult()
        @Suppress("UNCHECKED_CAST")
        val javaStyle = hashMapOf<String?, String?>(null to "orphan", "dropped" to null, "kept" to "1") as Map<String, String?>
        h.publisher.publishWithResult("t1", "k", event, javaStyle).awaitResult()
        assertThat(h.sender.calls.map { it.record.headers }).containsExactly(emptyMap(), mapOf("kept" to "1"))
    }

    // warnings about failing metrics are rate-limited (M3)

    @Test
    fun `flushWarnings also reports the failure handlers' held-back warnings`() {
        val throwing = PublishFailureHandler { throw IllegalStateException("handler down") }
        LogCapture(FailureDispatcher::class.java).use { logs ->
            val h = harness(sender = FakeRecordSender(FakeRecordSender.fail(RecordTooLargeException())), extraHandlers = listOf(throwing))
            repeat(3) { h.publisher.publishWithResult("t1", "k", event).awaitFailure() }
            h.publisher.flushWarnings()
            assertThat(logs.lines(Level.WARN, "kafka_publisher_failure_handler_failed_summary"))
                .singleElement().asString().startsWith("kafka_publisher_failure_handler_failed_summary channel=${h.channel} handler=").endsWith(" count=2 interval=PT10S")
        }
    }

    @Test
    fun `throwing delivery metrics are warned once per interval, the rest are summarised`() {
        val metrics = object : PublisherMetrics by PublisherMetrics.None {
            override fun delivered(channel: String, topic: String, latency: Duration) = throw IllegalStateException("metrics down")
        }
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            val h = Harness(metricsOverride = metrics).also { harnesses += it }
            repeat(3) { h.publisher.publishWithResult("t1", "k", event).awaitResult() }
            assertThat(logs.lines(Level.WARN)).containsExactly("kafka_publisher_metrics_failed channel=${h.channel}: java.lang.IllegalStateException: metrics down")

            h.publisher.flushWarnings()
            assertThat(logs.lines(Level.WARN).last()).isEqualTo("kafka_publisher_metrics_failed_summary channel=${h.channel} count=2 interval=PT10S")

            h.clock.advance(Duration.ofSeconds(10))
            h.publisher.publishWithResult("t1", "k", event).awaitResult()
            assertThat(logs.lines(Level.WARN, "kafka_publisher_metrics_failed channel=")).hasSize(2)
        }
    }

    // ── each stage maps to its failure, once ──────────────────────────────────────────────────────────

    @Test
    fun `serializer exception is SERIALIZATION on the caller, attempt 0, no payload`() {
        val boom = IllegalStateException("cannot serialize")
        val h = harness(serializer = EventSerializer { throw boom })

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(SERIALIZATION)
        assertThat(failure.attempt).isEqualTo(0)
        assertThat(failure.payload).isNull()
        assertThat(failure.cause).isSameAs(boom)
        assertThat(failure.retriable).isFalse()
        assertThat(h.handler.failures).containsExactly(failure)
        assertThat(h.handler.threads).containsExactly(Thread.currentThread().name)
        assertThat(h.metrics.failed).containsExactly(Triple(h.channel, "t1", SERIALIZATION))
        assertThat(h.sender.calls).isEmpty()
    }

    @Test
    fun `a serializer Error (a cyclic graph's StackOverflowError) is SERIALIZATION too, never thrown`() {
        val h = harness(serializer = EventSerializer { throw StackOverflowError() })
        assertDoesNotThrow { h.publisher.publish("t1", "k", event) }
        assertThat(h.handler.single().stage).isEqualTo(SERIALIZATION)
    }

    @Test
    fun `send that throws is SEND_REJECTED on the lane with attempt 1 and the payload`() {
        val h = harness(sender = FakeRecordSender(FakeRecordSender.throwing(IllegalStateException("producer closed"))))

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(SEND_REJECTED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(failure.payload).isEqualTo(json)
        assertThat(failure.cause).hasMessage("producer closed")
        assertThat(failure.retriable).isFalse()
        assertThat(h.handler.threads).containsExactly("${h.channel}-publisher-0")
    }

    @Test
    fun `callback error is DELIVERY_FAILED with attempt 1, the payload and the cause`() {
        val error = RecordTooLargeException("too large")
        val h = harness(sender = FakeRecordSender(FakeRecordSender.fail(error)))

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(failure.payload).isEqualTo(json)
        assertThat(failure.cause).isSameAs(error)
        assertThat(failure.channel).isEqualTo(h.channel)
        assertThat(failure.topic).isEqualTo("t1")
        assertThat(failure.key).isEqualTo("k")
        assertThat(failure.occurredAt).isEqualTo(h.clock.instant())
        assertThat(h.metrics.failed).containsExactly(Triple(h.channel, "t1", DELIVERY_FAILED))
    }

    @Test
    fun `full lane is QUEUE_FULL on the caller with attempt 0 and the payload`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val h = harness(
            settings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 1),
            sender = FakeRecordSender(FakeRecordSender.stuck(entered, release)),
        )
        h.publisher.publish("t1", "a", event)
        entered.await(5, TimeUnit.SECONDS)
        h.publisher.publish("t1", "b", event) // fills the one slot

        val failure = h.publisher.publishWithResult("t1", "c", event).awaitFailure()
        release.countDown()

        assertThat(failure.stage).isEqualTo(QUEUE_FULL)
        assertThat(failure.attempt).isEqualTo(0)
        assertThat(failure.payload).isEqualTo(json)
        assertThat(failure.cause).isNull()
        assertThat(failure.key).isEqualTo("c")
    }

    @Test
    fun `closed channel is CHANNEL_UNAVAILABLE on the caller and sends nothing`() {
        val h = harness()
        h.publisher.close()

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(CHANNEL_UNAVAILABLE)
        assertThat(failure.attempt).isEqualTo(0)
        assertThat(failure.payload).isNull()
        assertThat(h.sender.calls).isEmpty()
    }

    @Test
    fun `a defect inside the publisher is INTERNAL, never thrown`() {
        val defect = IllegalStateException("lane defect")
        val h = harness(backpressure = BackpressurePolicy { _, _ -> throw defect })

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(INTERNAL)
        assertThat(failure.cause).isSameAs(defect)
        assertThat(failure.attempt).isEqualTo(0)
        assertThat(h.handler.failures).hasSize(1)
    }

    @Test
    fun `no stage throws to the caller, even with throwing handlers and metrics`() {
        val throwingHandler = PublishFailureHandler { throw IllegalStateException("handler down") }
        val scenarios: List<Pair<String, () -> Harness>> = listOf(
            "serialization" to { harness(serializer = EventSerializer { throw IllegalArgumentException() }, extraHandlers = listOf(throwingHandler)) },
            "send rejected" to { harness(sender = FakeRecordSender(FakeRecordSender.throwing(IllegalStateException())), extraHandlers = listOf(throwingHandler)) },
            "delivery failed" to { harness(sender = FakeRecordSender(FakeRecordSender.fail(RecordTooLargeException())), extraHandlers = listOf(throwingHandler)) },
            "internal" to { harness(backpressure = BackpressurePolicy { _, _ -> throw IllegalStateException() }, extraHandlers = listOf(throwingHandler)) },
            "closed" to { harness(extraHandlers = listOf(throwingHandler)).also { it.publisher.close() } },
            "caller runs a throwing send" to {
                harness(
                    settings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 1, backpressure = BackpressureMode.CALLER_RUNS),
                    sender = FakeRecordSender(FakeRecordSender.throwing(IllegalStateException())),
                    backpressure = BackpressurePolicy { _, task -> task.run(); true },
                    extraHandlers = listOf(throwingHandler),
                )
            },
        )
        for ((name, build) in scenarios) {
            val h = build()
            assertDoesNotThrow({ h.publisher.publish("t1", "k", event) }, name)
            assertDoesNotThrow({ h.publisher.publishJson("t1", "k", "{}") }, name)
            assertDoesNotThrow({ h.publisher.publishWithResult("t1", "k", event) }, name)
            // each call ends exactly once (publishJson skips the serializer, so it is delivered in that scenario)
            eventually { h.handler.failures.size + h.metrics.delivered.size == 3 }
            assertThat(h.handler.failures).describedAs(name).hasSizeGreaterThanOrEqualTo(2)
        }
    }

    @Test
    fun `the caller is never blocked by a sender stuck in send`() {
        val entered = CountDownLatch(2)
        val release = CountDownLatch(1)
        val h = harness(
            settings = ChannelSettings(enabled = true, lanes = 2, queueCapacity = 4),
            sender = FakeRecordSender(FakeRecordSender.stuck(entered, release)),
        )
        val total = 2_000
        var slowestNanos = 0L
        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            repeat(total) { i ->
                val started = System.nanoTime()
                h.publisher.publish("t1", "player-$i", event)
                slowestNanos = maxOf(slowestNanos, System.nanoTime() - started)
            }
        }
        val dropped = h.handler.failures.count { it.stage == QUEUE_FULL }
        assertThat(dropped).isGreaterThanOrEqualTo(total - 2 * (2 + 1)) // at most 2 stuck + 2 queued per lane

        release.countDown()
        val deliveredOrDropped = { h.metrics.delivered.size + h.handler.failures.size }
        eventually { deliveredOrDropped() == total }
        assertThat(h.handler.failures).allMatch { it.stage == QUEUE_FULL }
        assertThat(TimeUnit.NANOSECONDS.toMillis(slowestNanos)).isLessThan(1_000)
    }

    // ── exactly once ──────────────────────────────────────────────────────────────────────────────────

    @Test
    fun `a sender that calls back and then throws settles the attempt once`() {
        val h = harness(sender = FakeRecordSender { _, _, onOutcome ->
            onOutcome(SendOutcome.Delivered(0, 7))
            throw IllegalStateException("late throw")
        })
        val result = h.publisher.publishWithResult("t1", "k", event).awaitResult()
        h.close()
        assertThat(result.offset).isEqualTo(7)
        assertThat(h.metrics.delivered).hasSize(1)
        assertThat(h.handler.failures).isEmpty()
    }

    @Test
    fun `a sender that calls back twice settles the attempt once`() {
        val h = harness(sender = FakeRecordSender { _, _, onOutcome ->
            onOutcome(SendOutcome.Failed(RecordTooLargeException()))
            onOutcome(SendOutcome.Delivered(0, 1))
        })
        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()
        h.close()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(h.handler.failures).hasSize(1)
        assertThat(h.metrics.delivered).isEmpty()
        assertThat(h.metrics.failed).hasSize(1)
    }

    @Test
    fun `a sender that throws after a failed callback reports one failure`() {
        val h = harness(sender = FakeRecordSender { _, _, onOutcome ->
            onOutcome(SendOutcome.Failed(RecordTooLargeException()))
            throw IllegalStateException("and throws")
        })
        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()
        h.close()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(h.handler.failures).hasSize(1)
    }

    @Test
    fun `throwing metrics on delivery still complete the future with the result`() {
        val metrics = object : PublisherMetrics by PublisherMetrics.None {
            override fun delivered(channel: String, topic: String, latency: Duration) = throw IllegalStateException("metrics down")
        }
        val h = Harness(metricsOverride = metrics).also { harnesses += it }

        assertThat(h.publisher.publishWithResult("t1", "k", event).awaitResult().topic).isEqualTo("t1")
        assertThat(h.handler.failures).isEmpty()
    }

    @Test
    fun `every published record ends exactly once as delivered or failed, per stage`() {
        val stages = mutableMapOf<PublishFailureStage?, Int>()
        val h = harness(sender = FakeRecordSender { record, _, onOutcome ->
            when (record.key) {
                "throw" -> throw IllegalStateException()
                "fail" -> onOutcome(SendOutcome.Failed(RecordTooLargeException()))
                else -> onOutcome(SendOutcome.Delivered(0, 1))
            }
        })
        val futures = listOf("ok", "throw", "fail", "ok", "fail").map { h.publisher.publishWithResult("t1", it, event) }
        futures.forEach { f ->
            val stage = try {
                f.awaitResult(); null
            } catch (e: Exception) {
                f.awaitFailure().stage
            }
            stages.merge(stage, 1, Int::plus)
        }
        assertThat(stages).isEqualTo(mapOf(null to 2, SEND_REJECTED to 1, DELIVERY_FAILED to 2))
        assertThat(h.handler.failures).hasSize(3)
        assertThat(h.metrics.failed).hasSize(3)
        assertThat(h.metrics.delivered).hasSize(2)
    }
}
