package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.Threads
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

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
            FakeRecordSender(FakeRecordSender.stuck(entered, release)),
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
