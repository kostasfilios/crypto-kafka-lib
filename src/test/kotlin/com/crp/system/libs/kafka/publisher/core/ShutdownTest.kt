package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.Threads
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ShutdownTest {
    private val event = SampleEvent("x", "1")

    @Test
    fun `close drains every queued record, then closes the producer with the time left`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val h = Harness(
            ChannelSettings(enabled = true, lanes = 2, queueCapacity = 20, shutdownTimeout = Duration.ofSeconds(10)),
            FakeRecordSender(FakeRecordSender.stuck(entered, release)),
        )
        val results = (1..8).map { h.publisher.publishWithResult("t1", "k$it", event) }
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

        val closing = Thread { h.publisher.close() }.apply { start() }
        eventually { closing.state == Thread.State.TIMED_WAITING } // close() is waiting for the lanes to drain
        assertThat(h.publisher.publishWithResult("t1", "late", event).awaitFailure().stage).isEqualTo(CHANNEL_UNAVAILABLE)
        assertThat(h.sender.closes).isEmpty() // the producer stays open while the lanes drain
        release.countDown()
        closing.join(10_000)
        assertThat(closing.isAlive).isFalse()

        results.forEach { it.awaitResult() } // nothing queued was lost
        assertThat(h.sender.callsBeforeClose).isEqualTo(8) // the producer closed only after the drain
        assertThat(h.sender.closes.single()).isGreaterThan(Duration.ZERO).isLessThanOrEqualTo(Duration.ofSeconds(10))
        assertThat(Threads.awaitGone("${h.channel}-publisher-")).isEmpty()
    }

    @Test
    fun `close gives up on a stuck lane at the shutdown timeout and closes the producer at once`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val h = Harness(
            ChannelSettings(enabled = true, lanes = 1, queueCapacity = 10, shutdownTimeout = Duration.ofMillis(300)),
            FakeRecordSender(FakeRecordSender.stuck(entered, release)).apply { onClose = { release.countDown() } },
        )
        try {
            h.publisher.publish("t1", "k", event)
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            val started = System.nanoTime()
            h.publisher.close()
            val tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

            assertThat(tookMillis).isBetween(250L, 5_000L)
            assertThat(h.sender.closes.single()).isLessThanOrEqualTo(Duration.ofMillis(100))
        } finally {
            release.countDown()
        }
    }

    @Test
    fun `records still queued at the deadline are failed as CHANNEL_UNAVAILABLE before close returns, and counted`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        // closing the producer wakes a send blocked in it, as KafkaProducer.close does
        val sender = FakeRecordSender(FakeRecordSender.stuck(entered, release)).apply { onClose = { release.countDown() } }
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 100, shutdownTimeout = Duration.ofMillis(200)), sender)
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            val results = (1..50).map { h.publisher.publishWithResult("t1", "k$it", event) }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

            h.publisher.close()

            assertThat(results).allMatch { it.isDone } // nothing outlives close()
            assertThat(results.first().awaitResult().key).isEqualTo("k1") // the in-flight send returned once the producer closed
            val abandoned = results.drop(1).map { it.awaitFailure() }
            assertThat(abandoned).allMatch { it.stage == CHANNEL_UNAVAILABLE && it.attempt == 0 && it.payload != null }
            assertThat(abandoned.map { it.key }).isEqualTo((2..50).map { "k$it" })
            assertThat(h.handler.failures).hasSize(49)
            assertThat(h.sender.calls).hasSize(1) // none of them was sent to the closed producer
            assertThat(logs.lines(Level.WARN)).containsExactly("kafka_publisher_shutdown_abandoned channel=${h.channel} count=49")
        }
    }

    @Test
    fun `only queued records are failed and counted at shutdown, another task in a lane is just dropped`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val sender = FakeRecordSender(FakeRecordSender.stuck(entered, release)).apply { onClose = { release.countDown() } }
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 10, shutdownTimeout = Duration.ofMillis(100)), sender)
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            h.publisher.publish("t1", "k1", event)
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
            val queued = (2..3).map { h.publisher.publishWithResult("t1", "k$it", event) }
            val foreignRan = AtomicBoolean(false)
            assertThat(h.lanes.forKey("k1").tryEnqueue { foreignRan.set(true) }).isTrue()

            assertDoesNotThrow { h.publisher.close() }

            assertThat(queued.map { it.awaitFailure().stage }).containsOnly(CHANNEL_UNAVAILABLE)
            assertThat(foreignRan.get()).isFalse()
            assertThat(logs.lines(Level.WARN)).containsExactly("kafka_publisher_shutdown_abandoned channel=${h.channel} count=2")
        }
    }

    @Test
    fun `close waits for a send that is still inside the producer when the producer closes`() {
        val entered = CountDownLatch(1)
        val finishInFlight = CountDownLatch(1)
        val producerClosed = AtomicBoolean(false)
        // unlike the other tests, closing does not release the send: it returns a moment later, as Kafka's do
        val sender = FakeRecordSender(FakeRecordSender.stuck(entered, finishInFlight)).apply { onClose = { producerClosed.set(true) } }
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 4, shutdownTimeout = Duration.ofMillis(100)), sender)
        val inFlight = h.publisher.publishWithResult("t1", "k", event)
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
        val closing = Thread.currentThread()
        // lets the send return only once close() has closed the producer and has been waiting for the lane for a while
        val helper = Thread {
            try {
                eventually { producerClosed.get() }
                var waitingSince = 0L
                eventually {
                    if (closing.state != Thread.State.TIMED_WAITING) {
                        waitingSince = 0
                        false
                    } else {
                        if (waitingSince == 0L) waitingSince = System.nanoTime()
                        System.nanoTime() - waitingSince > TimeUnit.MILLISECONDS.toNanos(20)
                    }
                }
            } finally {
                finishInFlight.countDown()
            }
        }.apply { isDaemon = true; start() }
        try {
            h.publisher.close()
            assertThat(inFlight.isDone).isTrue()
            assertThat(inFlight.awaitResult().key).isEqualTo("k")
        } finally {
            finishInFlight.countDown()
            helper.join(10_000)
        }
    }

    @Test
    fun `nothing abandoned means no WARN`() {
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            val h = Harness()
            h.publisher.publishWithResult("t1", "k", event).awaitResult()
            h.publisher.close()
            assertThat(logs.lines(Level.WARN)).isEmpty()
        }
    }

    @Test
    fun `close is idempotent and later publishes are CHANNEL_UNAVAILABLE`() {
        val h = Harness()
        h.publisher.close()
        h.publisher.close()
        assertThat(h.sender.closes).hasSize(1)
        assertThat(h.publisher.publishWithResult("t1", "k", event).awaitFailure().stage).isEqualTo(CHANNEL_UNAVAILABLE)
    }

    @Test
    fun `a negative shutdown timeout still stops the lanes and closes the producer`() {
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 4, shutdownTimeout = Duration.ofSeconds(-1)))
        h.publisher.close()
        assertThat(h.sender.closes.single()).isEqualTo(Duration.ZERO)
        assertThat(Threads.awaitGone("${h.channel}-publisher-")).isEmpty()
    }
}
