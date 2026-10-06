package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishResult
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
import com.crp.system.libs.kafka.publisher.testsupport.offerPastTheAcceptingCheck
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

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
    fun `the producer is closed only after every record queued at the deadline has been failed`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val queued = CopyOnWriteArrayList<CompletableFuture<PublishResult>>()
        val failedWhenClosed = AtomicInteger(-1)
        // closing the producer wakes the send that is blocked in it, as KafkaProducer.close does: the lane is free from that moment
        val sender = FakeRecordSender(FakeRecordSender.stuck(entered, release)).apply {
            onClose = {
                failedWhenClosed.set(queued.count { it.isDone })
                release.countDown()
            }
        }
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 100, shutdownTimeout = Duration.ofMillis(200)), sender)
        h.publisher.publish("t1", "k1", event)
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
        repeat(49) { queued += h.publisher.publishWithResult("t1", "k${it + 2}", event) }

        h.publisher.close()

        // all 49 were failed before the producer closed, so the freed lane had nothing left to send into it
        assertThat(failedWhenClosed.get()).isEqualTo(49)
        assertThat(queued.map { it.awaitFailure().stage }).containsOnly(CHANNEL_UNAVAILABLE)
        assertThat(h.sender.calls).hasSize(1)
    }

    @Test
    fun `a lane that is still draining at the deadline stops there, and every record is either sent or abandoned and counted`() {
        val sender = FakeRecordSender { record, index, onOutcome ->
            Thread.sleep(10) // a send takes a while, so 150 records cannot drain in 300 ms
            FakeRecordSender.deliver()(record, index, onOutcome)
        }
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 200, shutdownTimeout = Duration.ofMillis(300)), sender)
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            val results = (1..150).map { h.publisher.publishWithResult("t1", "k$it", event) }

            h.publisher.close()

            assertThat(results).allMatch { it.isDone } // nothing outlives close()
            val abandoned = results.filter { it.isCompletedExceptionally }.map { it.awaitFailure() }
            assertThat(abandoned).isNotEmpty()
            assertThat(abandoned).allMatch { it.stage == CHANNEL_UNAVAILABLE && it.attempt == 0 }
            assertThat(h.sender.calls.size + abandoned.size).isEqualTo(150) // none sent twice, none lost
            assertThat(logs.lines(Level.WARN)).containsExactly("kafka_publisher_shutdown_abandoned channel=${h.channel} count=${abandoned.size}")
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

    /** Admits like DROP, except that it keeps back the [heldCall]th record: the test puts that one into its lane later. */
    private class HoldBack(private val heldCall: Int) : BackpressurePolicy {
        private val calls = AtomicInteger()
        @Volatile var held: Pair<PublishLane, Runnable>? = null

        override fun admit(lane: PublishLane, task: Runnable): Boolean {
            if (calls.incrementAndGet() != heldCall) return lane.tryEnqueue(task)
            held = lane to task
            return true
        }
    }

    @Test
    fun `a record a worker had just polled when close halted the lanes is failed as CHANNEL_UNAVAILABLE and counted with the rest`() {
        // It takes an idle worker that is still polling when close() halts the lanes, and a record that lands in its queue
        // right after, as a publish that passed the accepting check just before close() would. The failure handler of the
        // first abandoned record puts it there. When the closing thread is held up for the rest of the worker's 100 ms
        // poll, the worker leaves first and the attempt is made again.
        val busyKey = "player-1"
        val idleKey = (2..1_000).map { "player-$it" }.first { Math.floorMod(it.hashCode(), 2) != Math.floorMod(busyKey.hashCode(), 2) }
        repeat(20) {
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val sender = FakeRecordSender(FakeRecordSender.stuckFor(busyKey, entered, release)).apply { onClose = { release.countDown() } }
            val holdBack = HoldBack(heldCall = 4)
            val offered = AtomicBoolean(false)
            val offerTheHeldRecord = PublishFailureHandler { failure ->
                val (lane, task) = holdBack.held!!
                if (failure.key == busyKey && offered.compareAndSet(false, true)) lane.offerPastTheAcceptingCheck(task)
            }
            val h = Harness(
                ChannelSettings(enabled = true, lanes = 2, queueCapacity = 20, shutdownTimeout = Duration.ZERO),
                sender,
                backpressure = holdBack,
                extraHandlers = listOf(offerTheHeldRecord),
            )
            LogCapture(ChannelEventPublisher::class.java).use { logs ->
                h.publisher.publish("t1", busyKey, event) // stuck in the producer
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
                val queued = (1..2).map { h.publisher.publishWithResult("t1", busyKey, event) } // queued behind it
                val late = h.publisher.publishWithResult("t1", idleKey, event) // the idle lane never gets it, until the handler offers it

                h.publisher.close()

                assertThat(queued.map { it.awaitFailure().stage }).containsOnly(CHANNEL_UNAVAILABLE)
                if (late.isDone) { // the idle worker polled it after the halt
                    assertThat(late.awaitFailure().stage).isEqualTo(CHANNEL_UNAVAILABLE)
                    assertThat(h.sender.calls).hasSize(1) // it was not sent
                    assertThat(logs.lines(Level.WARN)).containsExactly("kafka_publisher_shutdown_abandoned channel=${h.channel} count=3")
                    return
                }
            }
        }
        fail<Unit>("no attempt had the idle worker poll the record that was offered after the halt")
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
