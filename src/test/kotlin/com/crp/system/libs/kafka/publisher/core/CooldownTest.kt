package com.crp.system.libs.kafka.publisher.core

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.TopicCoolingDownException
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.spring.ProducerSettings
import com.crp.system.libs.kafka.publisher.spring.RetrySettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.SendScript
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.apache.kafka.clients.producer.BufferExhaustedException
import org.apache.kafka.common.errors.TimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Ruling L-3: a topic missing from the broker's metadata cools down instead of stalling its lanes. */
class CooldownTest {
    private val harnesses = mutableListOf<Harness>()
    private val event = SampleEvent("x", "1")

    @AfterEach
    fun closeAll() = harnesses.forEach { it.close() }

    /** "missing" behaves like a topic the broker does not have; every other topic delivers. */
    private fun missingTopicSender(missing: SendScript = FakeRecordSender.topicMissing("missing")) = FakeRecordSender { record, index, onOutcome ->
        if (record.topic == "missing") missing(record, index, onOutcome) else FakeRecordSender.deliver()(record, index, onOutcome)
    }

    private fun harness(
        sender: FakeRecordSender = missingTopicSender(),
        lanes: Int = 1,
        cooldown: Duration = Duration.ofSeconds(30),
        retry: RetrySettings = RetrySettings(),
    ) = Harness(
        ChannelSettings(enabled = true, lanes = lanes, queueCapacity = 64, retry = retry, producer = ProducerSettings(missingTopicCooldown = cooldown)),
        sender,
    ).also { harnesses += it }

    private fun FakeRecordSender.callsFor(topic: String) = calls.count { it.record.topic == topic }

    @Test
    fun `one missing topic never delays the lane's other topics beyond the first probe`() {
        val h = harness()
        val results = listOf("missing", "other", "missing", "other", "missing", "other").map { topic ->
            topic to h.publisher.publishWithResult(topic, "k", event)
        }

        val outcomes = results.map { (topic, future) -> topic to settle(future) }

        assertThat(h.sender.callsFor("missing")).isEqualTo(1) // only the first one reached the producer
        assertThat(outcomes.filter { it.first == "other" }.map { it.second }).allMatch { it is PublishResult }
        val missing = outcomes.filter { it.first == "missing" }.map { it.second as PublishFailure }
        assertThat(missing[0].cause).isInstanceOf(TimeoutException::class.java)
        assertThat(missing[0].attempt).isEqualTo(1)
        missing.drop(1).forEach { failure ->
            assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
            assertThat(failure.retriable).isFalse()
            assertThat(failure.attempt).isEqualTo(0) // never sent
            assertThat(failure.payload).isNotNull()
            val cause = failure.cause as TopicCoolingDownException
            assertThat(cause.topic).isEqualTo("missing")
            assertThat(cause.until).isEqualTo(h.clock.instant().plusSeconds(30))
            assertThat(cause.stackTrace).isEmpty()
        }
    }

    private fun settle(future: CompletableFuture<PublishResult>): Any =
        try {
            future.awaitResult()
        } catch (e: Exception) {
            future.awaitFailure()
        }

    @Test
    fun `after the cool-down one send probes the topic again`() {
        val h = harness()
        h.publisher.publishWithResult("missing", "k", event).awaitFailure()
        h.clock.advance(Duration.ofSeconds(29))
        assertThat(h.publisher.publishWithResult("missing", "k", event).awaitFailure().cause).isInstanceOf(TopicCoolingDownException::class.java)
        assertThat(h.sender.callsFor("missing")).isEqualTo(1)

        h.clock.advance(Duration.ofSeconds(1))
        val probe = h.publisher.publishWithResult("missing", "k", event).awaitFailure()

        assertThat(h.sender.callsFor("missing")).isEqualTo(2)
        assertThat(probe.cause).isInstanceOf(TimeoutException::class.java) // still missing: it cools down again
        assertThat(h.publisher.publishWithResult("missing", "k", event).awaitFailure().cause).isInstanceOf(TopicCoolingDownException::class.java)
        assertThat(h.sender.callsFor("missing")).isEqualTo(2)
    }

    @Test
    fun `a probe that finds the topic ends the cool-down`() {
        val created = AtomicBoolean(false)
        val h = harness(missingTopicSender { record, index, onOutcome ->
            if (created.get()) FakeRecordSender.deliver()(record, index, onOutcome) else FakeRecordSender.topicMissing("missing")(record, index, onOutcome)
        })
        h.publisher.publishWithResult("missing", "k", event).awaitFailure()
        created.set(true) // someone created the topic
        h.clock.advance(Duration.ofSeconds(30))

        h.publisher.publishWithResult("missing", "k", event).awaitResult()
        h.publisher.publishWithResult("missing", "k", event).awaitResult()

        assertThat(h.sender.callsFor("missing")).isEqualTo(3)
    }

    @Test
    fun `while one lane probes, the other lanes keep failing fast`() {
        val probing = CountDownLatch(1)
        val releaseProbe = CountDownLatch(1)
        val h = harness(
            lanes = 2,
            sender = missingTopicSender { record, index, onOutcome ->
                if (index > 0) { probing.countDown(); releaseProbe.await(10, TimeUnit.SECONDS) } // the probe blocks like max-block-ms
                FakeRecordSender.topicMissing("missing")(record, index, onOutcome)
            },
        )
        val (keyA, keyB) = keysOnDifferentLanes()
        try {
            h.publisher.publishWithResult("missing", keyA, event).awaitFailure()
            h.clock.advance(Duration.ofSeconds(30))

            val probe = h.publisher.publishWithResult("missing", keyA, event)
            assertThat(probing.await(5, TimeUnit.SECONDS)).isTrue()
            val other = h.publisher.publishWithResult("missing", keyB, event).awaitFailure()

            assertThat(other.cause).isInstanceOf(TopicCoolingDownException::class.java)
            assertThat(h.sender.callsFor("missing")).isEqualTo(2)
            releaseProbe.countDown()
            assertThat(probe.awaitFailure().cause).isInstanceOf(TimeoutException::class.java)
        } finally {
            releaseProbe.countDown()
        }
    }

    private fun keysOnDifferentLanes(): Pair<String, String> {
        val first = "player-1"
        val second = (2..1_000).map { "player-$it" }.first { Math.floorMod(it.hashCode(), 2) != Math.floorMod(first.hashCode(), 2) }
        return first to second
    }

    @Test
    fun `a full buffer does not start a cool-down`() {
        val h = harness(missingTopicSender { _, _, onOutcome ->
            onOutcome(SendOutcome.Failed(BufferExhaustedException("Failed to allocate memory within the configured max blocking time 2000 ms.")))
        })
        repeat(3) { h.publisher.publishWithResult("missing", "k", event).awaitFailure() }
        assertThat(h.sender.callsFor("missing")).isEqualTo(3)
    }

    @Test
    fun `a delivery timeout reported later by the producer does not start a cool-down`() {
        val h = harness(missingTopicSender { _, _, onOutcome ->
            onOutcome(SendOutcome.Failed(TimeoutException("Expiring 1 record(s) for missing-0: 30000 ms has passed since batch creation")))
        })
        repeat(3) { h.publisher.publishWithResult("missing", "k", event).awaitFailure() }
        assertThat(h.sender.callsFor("missing")).isEqualTo(3)
    }

    @Test
    fun `a cool-down of 0 turns it off`() {
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            val h = harness(cooldown = Duration.ZERO)
            repeat(3) { h.publisher.publishWithResult("missing", "k", event).awaitFailure() }
            assertThat(h.sender.callsFor("missing")).isEqualTo(3)
            assertThat(logs.lines(Level.WARN)).isEmpty()
        }
    }

    /** Monotonic time that, once armed, holds [parties] callers and releases them together (spinning, not parking). */
    private class RacingNanos(@Volatile var now: Long) {
        @Volatile private var parties = 0
        @Volatile private var go = false
        private val arrived = java.util.concurrent.atomic.AtomicInteger()

        fun arm(count: Int) {
            arrived.set(0)
            go = false
            parties = count
        }

        fun read(): Long {
            val waitingFor = parties
            if (waitingFor > 0) {
                if (arrived.incrementAndGet() == waitingFor) {
                    parties = 0
                    go = true
                }
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
                while (!go && System.nanoTime() < deadline) Thread.onSpinWait()
            }
            return now
        }
    }

    @Test
    fun `two lanes that reach an expired cool-down at the same instant send one probe`() {
        val nanos = RacingNanos(1_000_000_000L)
        val sender = missingTopicSender()
        val h = Harness(
            ChannelSettings(enabled = true, lanes = 2, queueCapacity = 64, producer = ProducerSettings(missingTopicCooldown = Duration.ofSeconds(30))),
            sender,
            nanoTimeOverride = nanos::read,
        ).also { harnesses += it }
        val (keyA, keyB) = keysOnDifferentLanes()
        h.publisher.publishWithResult("missing", keyA, event).awaitFailure() // the topic cools down
        nanos.now += TimeUnit.SECONDS.toNanos(30) // and the cool-down is over

        val gate = CountDownLatch(1)
        val gated = CountDownLatch(2)
        listOf(keyA, keyB).forEach { key -> h.lanes.forKey(key).tryEnqueue { gated.countDown(); gate.await(10, TimeUnit.SECONDS) } }
        assertThat(gated.await(5, TimeUnit.SECONDS)).isTrue() // both lane workers are held
        val results = listOf(keyA, keyB).map { h.publisher.publishWithResult("missing", it, event) }
        nanos.arm(2) // both lanes read the same expired deadline, then race for the probe
        gate.countDown()

        val causes = results.map { it.awaitFailure().cause }
        assertThat(h.sender.callsFor("missing")).isEqualTo(2) // the first send, then exactly one probe
        assertThat(causes.filterIsInstance<TopicCoolingDownException>()).hasSize(1)
        assertThat(causes.filterIsInstance<TimeoutException>()).hasSize(1)
    }

    @Test
    fun `the periodic sweep forgets a cool-down that is over, and keeps one that is still running`() {
        val sender = FakeRecordSender { record, index, onOutcome ->
            val script = if (record.topic.startsWith("missing")) FakeRecordSender.topicMissing(record.topic) else FakeRecordSender.deliver()
            script(record, index, onOutcome)
        }
        val h = harness(sender)
        h.publisher.publishWithResult("missing-a", "k", event).awaitFailure() // cools down for 30 s
        h.clock.advance(Duration.ofSeconds(20))
        h.publisher.publishWithResult("missing-b", "k", event).awaitFailure() // 20 s later, so its cool-down ends 20 s after a's
        h.clock.advance(Duration.ofSeconds(10)) // a's ends exactly now (30 s), b's has 20 s to go
        assertThat(h.publisher.coolingDownTopics()).containsExactlyInAnyOrder("missing-a", "missing-b") // nobody has sent to either since

        h.publisher.expireCooldowns()

        assertThat(h.publisher.coolingDownTopics()).containsExactly("missing-b")
        assertThat(h.publisher.publishWithResult("missing-b", "k", event).awaitFailure().cause).isInstanceOf(TopicCoolingDownException::class.java)
        assertThat(h.sender.callsFor("missing-b")).isEqualTo(1) // still failing at once
        h.publisher.publishWithResult("missing-a", "k", event).awaitFailure() // forgotten: sent like a new topic, found missing, cooling down again
        assertThat(h.sender.callsFor("missing-a")).isEqualTo(2)
        assertThat(h.publisher.coolingDownTopics()).containsExactlyInAnyOrder("missing-a", "missing-b")
    }

    @Test
    fun `the sweep leaves the probe's own entry alone, so the other lanes keep failing fast while it runs`() {
        val probing = CountDownLatch(1)
        val releaseProbe = CountDownLatch(1)
        val h = harness(
            lanes = 2,
            sender = missingTopicSender { record, index, onOutcome ->
                if (index > 0) { probing.countDown(); releaseProbe.await(10, TimeUnit.SECONDS) } // the probe blocks like max-block-ms
                FakeRecordSender.topicMissing("missing")(record, index, onOutcome)
            },
        )
        val (keyA, keyB) = keysOnDifferentLanes()
        try {
            h.publisher.publishWithResult("missing", keyA, event).awaitFailure()
            h.clock.advance(Duration.ofSeconds(30))
            val probe = h.publisher.publishWithResult("missing", keyA, event)
            assertThat(probing.await(5, TimeUnit.SECONDS)).isTrue() // the probe has taken the entry: a deadline 30 s ahead

            h.publisher.expireCooldowns() // the tick comes while the probe is still blocked

            val other = h.publisher.publishWithResult("missing", keyB, event).awaitFailure()
            assertThat(other.cause).isInstanceOf(TopicCoolingDownException::class.java)
            assertThat(h.sender.callsFor("missing")).isEqualTo(2) // the first send and the probe: the election still holds
            releaseProbe.countDown()
            assertThat(probe.awaitFailure().cause).isInstanceOf(TimeoutException::class.java)
        } finally {
            releaseProbe.countDown()
        }
    }

    /**
     * Monotonic time that holds the next read of one thread until [release]. It stops a lane inside cooldownFor, after
     * its read of the topic's deadline and before its replace of it, which is where two lanes race for the probe.
     */
    private class PausedNanos(private val clock: MutableClock) {
        @Volatile private var thread: String? = null
        private val reached = CountDownLatch(1)
        private val released = CountDownLatch(1)

        fun pauseNextReadOn(threadName: String) {
            thread = threadName
        }

        fun awaitReached(): Boolean = reached.await(5, TimeUnit.SECONDS)

        fun release() = released.countDown()

        fun read(): Long {
            if (Thread.currentThread().name == thread) {
                thread = null
                reached.countDown()
                released.await(10, TimeUnit.SECONDS)
            }
            return clock.nanoTime()
        }
    }

    private fun racingHarness(clock: MutableClock, nanos: PausedNanos, channel: String, sender: FakeRecordSender) = Harness(
        ChannelSettings(enabled = true, lanes = 2, queueCapacity = 64, producer = ProducerSettings(missingTopicCooldown = Duration.ofSeconds(30))),
        sender,
        clock = clock,
        nanoTimeOverride = nanos::read,
        channel = channel,
    ).also { harnesses += it }

    @Test
    fun `a send that loses the probe to a send that found the topic back sends normally`() {
        val clock = MutableClock()
        val nanos = PausedNanos(clock)
        val created = AtomicBoolean(false)
        val h = racingHarness(clock, nanos, "lost-to-success", missingTopicSender { record, index, onOutcome ->
            if (created.get()) FakeRecordSender.deliver()(record, index, onOutcome) else FakeRecordSender.topicMissing("missing")(record, index, onOutcome)
        })
        val (keyA, keyB) = keysOnDifferentLanes()
        h.publisher.publishWithResult("missing", keyA, event).awaitFailure() // the topic cools down
        clock.advance(Duration.ofSeconds(30)) // and the cool-down is over
        created.set(true) // someone created the topic
        nanos.pauseNextReadOn(h.lanes.forKey(keyA).name)

        val loser = h.publisher.publishWithResult("missing", keyA, event) // its lane has read the expired deadline, and waits
        assertThat(nanos.awaitReached()).isTrue()
        h.publisher.publishWithResult("missing", keyB, event).awaitResult() // the other lane probes and finds the topic back...
        eventually { h.publisher.coolingDownTopics().isEmpty() } // ...and drops the entry once its send returns, just after the future
        nanos.release()

        loser.awaitResult() // its replace finds no entry: nothing cools down, it sends like any other send
        assertThat(h.sender.callsFor("missing")).isEqualTo(3)
        assertThat(h.publisher.coolingDownTopics()).isEmpty()
    }

    @Test
    fun `a send that loses the probe to one whose new cool-down is already over sends normally`() {
        val clock = MutableClock()
        val nanos = PausedNanos(clock)
        val h = racingHarness(clock, nanos, "lost-to-expired", missingTopicSender())
        val (keyA, keyB) = keysOnDifferentLanes()
        h.publisher.publishWithResult("missing", keyA, event).awaitFailure() // the topic cools down
        clock.advance(Duration.ofSeconds(30)) // and the cool-down is over
        nanos.pauseNextReadOn(h.lanes.forKey(keyA).name)

        val loser = h.publisher.publishWithResult("missing", keyA, event) // its lane has read the expired deadline, and waits
        assertThat(nanos.awaitReached()).isTrue()
        assertThat(h.publisher.publishWithResult("missing", keyB, event).awaitFailure().cause).isInstanceOf(TimeoutException::class.java) // the other lane's probe: still missing
        clock.advance(Duration.ofSeconds(30)) // the cool-down that probe started is over as well when the first lane goes on
        nanos.release()

        assertThat(loser.awaitFailure().cause).isInstanceOf(TimeoutException::class.java) // it was sent, not failed at once
        assertThat(h.sender.callsFor("missing")).isEqualTo(3)
    }

    @Test
    fun `starting a cool-down logs one WARN`() {
        LogCapture(ChannelEventPublisher::class.java).use { logs ->
            val h = harness()
            repeat(3) { h.publisher.publishWithResult("missing", "k", event).awaitFailure() }
            assertThat(logs.lines(Level.WARN)).containsExactly("kafka_publisher_topic_cooling_down channel=${h.channel} topic=missing for=PT30S")
        }
    }

    @Test
    fun `app retries do not hammer a topic that is cooling down`() {
        val h = harness(retry = RetrySettings(maxAttempts = 3))
        val result = h.publisher.publishWithResult("missing", "k", event)
        h.scheduler.runNext() // the retry finds the topic cooling down: it fails at once and is not retried

        val failure = result.awaitFailure()
        assertThat(failure.cause).isInstanceOf(TopicCoolingDownException::class.java)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(h.sender.callsFor("missing")).isEqualTo(1)
        eventually { h.scheduler.pendingCount() == 0 }
    }
}
