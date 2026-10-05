package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.spring.OrderingMode
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** PER_KEY: the key's hash picks the lane, so one key always uses one FIFO lane. NONE or a null key: round-robin. */
internal class PublishLanes(channel: String, count: Int, totalCapacity: Int, private val ordering: OrderingMode) {
    private val lanes = List(count) { index -> PublishLane("$channel-publisher-$index", (totalCapacity + count - 1) / count) }
    private val next = AtomicInteger()

    fun forKey(key: String?): PublishLane =
        if (ordering == OrderingMode.PER_KEY && key != null) lanes[Math.floorMod(key.hashCode(), lanes.size)]
        else lanes[Math.floorMod(next.getAndIncrement(), lanes.size)]

    fun queued(): Int = lanes.sumOf { it.queued() }

    /** Stop accepting, then let every lane drain until [deadlineNanos] (a `System.nanoTime()` deadline). */
    fun drainAndStop(deadlineNanos: Long) {
        lanes.forEach { it.stopAccepting() }
        lanes.forEach { it.awaitDrained(deadlineNanos) }
    }
}

/** One worker thread over one bounded queue. JDK only: no Reactor in the lib. */
internal class PublishLane(val name: String, capacity: Int) {
    private val queue = ArrayBlockingQueue<Runnable>(capacity)
    @Volatile private var accepting = true
    private val worker = Thread(::work, name).apply { isDaemon = true; start() }

    fun tryEnqueue(task: Runnable): Boolean = admitted(task, accepting && queue.offer(task))

    fun enqueueWaiting(task: Runnable, timeout: Duration): Boolean = admitted(task, accepting && offerWaiting(task, timeout))

    fun queued(): Int = queue.size

    fun stopAccepting() {
        accepting = false
    }

    fun awaitDrained(deadlineNanos: Long) {
        try {
            worker.join(TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()).coerceAtLeast(1))
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /** A close that raced the offer may have let the worker exit already: take the task back so the caller reports it. */
    private fun admitted(task: Runnable, offered: Boolean): Boolean = offered && (accepting || !queue.remove(task))

    private fun offerWaiting(task: Runnable, timeout: Duration): Boolean =
        try {
            queue.offer(task, timeout.toNanos(), TimeUnit.NANOSECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt() // the caller keeps its interrupt status; the record is reported QUEUE_FULL
            false
        }

    private fun work() {
        while (accepting || queue.isNotEmpty()) {
            val task = try {
                queue.poll(100, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                null
            } ?: continue
            try {
                task.run()
            } catch (e: Throwable) {
                // tasks guard themselves; this keeps the lane alive
                logger.error("kafka_publisher_lane_task_failed lane={}: {}", name, e.toString())
            }
        }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(PublishLane::class.java)
    }
}
