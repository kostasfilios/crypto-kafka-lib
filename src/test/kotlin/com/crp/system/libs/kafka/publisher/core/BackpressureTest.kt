package com.crp.system.libs.kafka.publisher.core

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.adapters.LoggingPublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.spring.BackpressureMode
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** One lane with one slot and a sender stuck on key "stuck": the lane is full after two publishes. */
class BackpressureTest {
    private val harnesses = mutableListOf<Harness>()
    private val entered = CountDownLatch(1)
    private val release = CountDownLatch(1)
    private val event = SampleEvent("x", "1")

    @AfterEach
    fun cleanUp() {
        release.countDown()
        harnesses.forEach { it.close() }
    }

    private fun fullLane(mode: BackpressureMode, blockTimeout: Duration = Duration.ofMillis(100), clock: MutableClock = MutableClock(), logging: LoggingPublishFailureHandler? = null): Harness {
        val h = Harness(
            settings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 1, backpressure = mode, blockTimeout = blockTimeout),
            sender = FakeRecordSender(FakeRecordSender.stuckFor("stuck", entered, release)),
            clock = clock,
            extraHandlers = listOfNotNull(logging),
        ).also { harnesses += it }
        h.publisher.publish("t1", "stuck", event)
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
        h.publisher.publish("t1", "queued", event)
        eventually { h.lanes.queued() == 1 }
        return h
    }

    @Test
    fun `DROP drops on the caller without waiting, and the logging handler gives one summary with the count`() {
        val clock = MutableClock()
        val logging = LoggingPublishFailureHandler(Duration.ofSeconds(10), clock::nanoTime)
        LogCapture(LoggingPublishFailureHandler::class.java).use { logs ->
            val h = fullLane(BackpressureMode.DROP, clock = clock, logging = logging)

            val started = System.nanoTime()
            repeat(10) { h.publisher.publish("t1", "dropped-$it", event) }
            val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

            assertThat(h.handler.awaitFailures(10)).allMatch { it.stage == QUEUE_FULL }
            assertThat(h.handler.threads).allMatch { it == Thread.currentThread().name }
            assertThat(elapsedMillis).isLessThan(1_000)
            assertThat(logs.lines(Level.ERROR)).isEmpty() // drops are only counted

            logging.flushSummaries()
            assertThat(logs.lines(Level.WARN, "kafka_publish_failed_summary"))
                .containsExactly("kafka_publish_failed_summary channel=${h.channel} topic=t1 stage=QUEUE_FULL count=10 interval=PT10S")
        }
    }

    @Test
    fun `CALLER_RUNS runs the send on the caller when the lane is full`() {
        val h = fullLane(BackpressureMode.CALLER_RUNS)

        val result = h.publisher.publishWithResult("t1", "overflow", event).awaitResult()

        val call = h.sender.calls.single { it.record.key == "overflow" }
        assertThat(call.thread).isEqualTo(Thread.currentThread().name)
        assertThat(result.key).isEqualTo("overflow")
        assertThat(h.handler.failures).isEmpty()
    }

    @Test
    fun `BLOCK_WITH_TIMEOUT waits the block timeout, then reports QUEUE_FULL`() {
        val h = fullLane(BackpressureMode.BLOCK_WITH_TIMEOUT, blockTimeout = Duration.ofMillis(150))

        val started = System.nanoTime()
        val failure = h.publisher.publishWithResult("t1", "late", event).awaitFailure()
        val waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

        assertThat(failure.stage).isEqualTo(QUEUE_FULL)
        assertThat(failure.key).isEqualTo("late")
        assertThat(waitedMillis).isBetween(140L, 5_000L)
        assertThat(h.handler.threads.single()).isEqualTo(Thread.currentThread().name)
    }

    @Test
    fun `BLOCK_WITH_TIMEOUT admits the record when the lane frees up during the wait`() {
        val h = fullLane(BackpressureMode.BLOCK_WITH_TIMEOUT, blockTimeout = Duration.ofSeconds(30))
        val result = CompletableFuture<CompletableFuture<*>>()
        val caller = Thread { result.complete(h.publisher.publishWithResult("t1", "waiting", event)) }.apply { start() }

        eventually { caller.state == Thread.State.TIMED_WAITING } // parked in the bounded offer
        release.countDown()

        assertThat(result.awaitResult().awaitResult()).isNotNull
        assertThat(h.handler.failures).isEmpty()
    }

    @Test
    fun `BLOCK_WITH_TIMEOUT keeps the caller's interrupt status and reports QUEUE_FULL at once`() {
        val h = fullLane(BackpressureMode.BLOCK_WITH_TIMEOUT, blockTimeout = Duration.ofSeconds(30))

        Thread.currentThread().interrupt()
        val started = System.nanoTime()
        h.publisher.publish("t1", "interrupted", event)
        val stillInterrupted = Thread.interrupted() // also clears it for the rest of the test

        assertThat(stillInterrupted).isTrue()
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(5_000)
        assertThat(h.handler.single().stage).isEqualTo(QUEUE_FULL)
    }

    @Test
    fun `BLOCK_WITH_TIMEOUT admits a caller with a pending interrupt when the lane has room, and keeps the interrupt`() {
        val h = Harness(settings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 4, backpressure = BackpressureMode.BLOCK_WITH_TIMEOUT))
            .also { harnesses += it }

        Thread.currentThread().interrupt()
        val result = h.publisher.publishWithResult("t1", "interrupted-but-room", event)
        val stillInterrupted = Thread.interrupted()

        assertThat(stillInterrupted).isTrue()
        assertThat(result.awaitResult().key).isEqualTo("interrupted-but-room")
        assertThat(h.handler.failures).isEmpty()
    }

    @Test
    fun `under DROP a lane with room never drops`() {
        val h = Harness(settings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 64)).also { harnesses += it }
        val results = (1..50).map { h.publisher.publishWithResult("t1", "k", event) }
        results.forEach { it.awaitResult() }
        assertThat(h.handler.failures).isEmpty()
    }
}
