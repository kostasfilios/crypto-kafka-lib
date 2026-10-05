package com.crp.system.libs.kafka.publisher.testsupport

import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.core.OutboundRecord
import com.crp.system.libs.kafka.publisher.core.RecordSender
import com.crp.system.libs.kafka.publisher.core.SendOutcome
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport

const val WAIT_SECONDS = 10L

/** A Clock the test moves by hand, with a monotonic counterpart ([nanoTime]) that only moves forward with [advance]. */
class MutableClock(start: Instant = Instant.parse("2026-10-05T12:00:00Z"), startNanos: Long = 1_000_000_000L) : Clock() {
    private val now = AtomicReference(start)
    private val nanos = AtomicLong(startNanos)
    override fun instant(): Instant = now.get()
    override fun getZone(): ZoneId = ZoneOffset.UTC
    override fun withZone(zone: ZoneId?): Clock = this

    /** The monotonic source to inject where the code would call `System.nanoTime()`. */
    fun nanoTime(): Long = nanos.get()

    /** Time passes: the wall clock and the monotonic clock both move. */
    fun advance(by: Duration) {
        now.updateAndGet { it.plus(by) }
        nanos.addAndGet(by.toNanos())
    }

    /** Only the wall clock jumps (NTP, a VM resume); monotonic time does not. */
    fun stepWallClock(by: Duration) {
        now.updateAndGet { it.plus(by) }
    }
}

internal typealias SendScript = (record: OutboundRecord, callIndex: Int, onOutcome: (SendOutcome) -> Unit) -> Unit

/** A RecordSender driven by a script. Records every call with its thread. */
internal class FakeRecordSender(@Volatile var script: SendScript = deliver()) : RecordSender {
    data class Call(val record: OutboundRecord, val thread: String)

    val calls = CopyOnWriteArrayList<Call>()
    private val callQueue = LinkedBlockingQueue<Call>()
    val closes = CopyOnWriteArrayList<Duration>()
    @Volatile var callsBeforeClose = -1

    override fun send(record: OutboundRecord, onOutcome: (SendOutcome) -> Unit) {
        val call = Call(record, Thread.currentThread().name)
        val index = synchronized(calls) { calls += call; calls.size - 1 }
        callQueue += call
        script(record, index, onOutcome)
    }

    /** Runs on close, like a real producer whose close wakes a send blocked in it. */
    @Volatile var onClose: () -> Unit = {}

    override fun close(timeout: Duration) {
        callsBeforeClose = calls.size
        closes += timeout
        onClose()
    }

    /** Waits for the next send call (in call order). */
    fun nextCall(): Call = callQueue.poll(WAIT_SECONDS, TimeUnit.SECONDS) ?: error("no send call within $WAIT_SECONDS s")

    fun keys(): List<String?> = calls.map { it.record.key }

    companion object {
        private val offsets = AtomicLong()

        /** Delivers at once, on the calling thread, to partition 0 with an increasing offset. */
        fun deliver(partition: Int = 0): SendScript = { _, _, onOutcome -> onOutcome(SendOutcome.Delivered(partition, offsets.incrementAndGet())) }

        fun fail(error: Exception): SendScript = { _, _, onOutcome -> onOutcome(SendOutcome.Failed(error)) }

        /** What the Kafka sender reports, inside send(), for a topic missing from the broker's metadata after max-block-ms. */
        fun topicMissing(topic: String = "t1"): SendScript = { _, _, onOutcome ->
            onOutcome(SendOutcome.Failed(org.apache.kafka.common.errors.TimeoutException("Topic $topic not present in metadata after 2000 ms."), topicMissing = true))
        }

        fun throwing(error: Exception): SendScript = { _, _, _ -> throw error }

        /** One script per call index; the last one repeats. */
        fun sequence(vararg scripts: SendScript): SendScript = { record, index, onOutcome ->
            scripts[minOf(index, scripts.size - 1)](record, index, onOutcome)
        }

        /** Blocks the lane in `send` until [release] opens, after counting down [entered]; then runs [then]. */
        fun stuck(entered: CountDownLatch, release: CountDownLatch, then: SendScript = deliver()): SendScript = { record, index, onOutcome ->
            entered.countDown()
            release.await(WAIT_SECONDS * 3, TimeUnit.SECONDS)
            then(record, index, onOutcome)
        }

        /** Blocks only the records whose key matches. */
        fun stuckFor(key: String, entered: CountDownLatch, release: CountDownLatch): SendScript {
            val blocking = stuck(entered, release)
            val normal = deliver()
            return { record, index, onOutcome -> (if (record.key == key) blocking else normal)(record, index, onOutcome) }
        }
    }
}

/** Collects failures; [awaitFailures] blocks until n arrived. */
class RecordingFailureHandler : PublishFailureHandler {
    val failures = CopyOnWriteArrayList<PublishFailure>()
    val threads = CopyOnWriteArrayList<String>()
    private val arrivals = LinkedBlockingQueue<PublishFailure>()

    override fun onFailure(failure: PublishFailure) {
        failures += failure
        threads += Thread.currentThread().name
        arrivals += failure
    }

    fun awaitFailures(count: Int): List<PublishFailure> = List(count) {
        arrivals.poll(WAIT_SECONDS, TimeUnit.SECONDS) ?: error("only $it of $count failures within $WAIT_SECONDS s: $failures")
    }

    fun single(): PublishFailure = awaitFailures(1).single()
}

class RecordingMetrics : PublisherMetrics {
    val delivered = CopyOnWriteArrayList<Triple<String, String, Duration>>()
    val failed = CopyOnWriteArrayList<Triple<String, String, PublishFailureStage>>()
    val gauges = ConcurrentHashMap<String, () -> Int>()

    override fun delivered(channel: String, topic: String, latency: Duration) {
        delivered += Triple(channel, topic, latency)
    }

    override fun failed(channel: String, topic: String, stage: PublishFailureStage) {
        failed += Triple(channel, topic, stage)
    }

    override fun queueDepth(channel: String, depth: () -> Int) {
        gauges[channel] = depth
    }
}

object Threads {
    fun named(prefix: String): List<String> = Thread.getAllStackTraces().keys.filter { it.isAlive && it.name.startsWith(prefix) }.map { it.name }

    /** Waits until no live thread has [prefix] (threads that are stopping finish their loop first). */
    fun awaitGone(prefix: String, timeout: Duration = Duration.ofSeconds(WAIT_SECONDS)): List<String> {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            val alive = Thread.getAllStackTraces().keys.filter { it.isAlive && it.name.startsWith(prefix) }
            if (alive.isEmpty()) return emptyList()
            alive.first().join(50)
        }
        return named(prefix)
    }
}

/** Polls [condition] until true (for asynchronous effects with no completion signal of their own). */
fun eventually(timeout: Duration = Duration.ofSeconds(WAIT_SECONDS), condition: () -> Boolean) {
    val deadline = System.nanoTime() + timeout.toNanos()
    while (!condition()) {
        if (System.nanoTime() > deadline) throw AssertionError("condition not met within $timeout")
        LockSupport.parkNanos(1_000_000)
    }
}
