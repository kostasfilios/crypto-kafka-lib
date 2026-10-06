package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.INTERNAL
import com.crp.system.libs.kafka.publisher.spring.OrderingMode
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/** PER_KEY: the key's hash picks the lane, so one key always uses one FIFO lane. NONE or a null key: round-robin. */
internal class PublishLanes(channel: String, count: Int, totalCapacity: Int, private val ordering: OrderingMode) {
    private val lanes = List(count) { index -> PublishLane("$channel-publisher-$index", (totalCapacity + count - 1) / count) }
    private val next = AtomicInteger()

    fun forKey(key: String?): PublishLane =
        if (ordering == OrderingMode.PER_KEY && key != null) lanes[Math.floorMod(key.hashCode(), lanes.size)]
        else lanes[Math.floorMod(next.getAndIncrement(), lanes.size)]

    fun queued(): Int = lanes.sumOf { it.queued() }

    fun stopAccepting() = lanes.forEach { it.stopAccepting() }

    /** Lets every lane drain until [deadlineNanos] (a `System.nanoTime()` deadline). */
    fun awaitDrained(deadlineNanos: Long) = lanes.forEach { it.awaitDrained(deadlineNanos) }

    /**
     * The shutdown cut-off. Every lane is halted first, so no worker starts another task; only then is what is still
     * queued taken. The result is what was queued when the lanes stopped, whatever the workers were doing. (A task a
     * worker had just polled is given up by the worker itself, see [PublishLane.halt].)
     */
    fun haltAndTakeRemaining(): List<Runnable> {
        lanes.forEach { it.halt() }
        return lanes.flatMap { it.takeRemaining() }
    }
}

/** One worker thread over one bounded queue. JDK only: no Reactor in the lib. */
internal class PublishLane(val name: String, capacity: Int) {
    private val queue = ArrayBlockingQueue<Runnable>(capacity)
    @Volatile private var accepting = true

    /** Set by [halt]: the worker starts no further task, whatever is still queued. */
    @Volatile private var halted = false
    private val restarted = AtomicBoolean(false)
    @Volatile private var worker: Thread = startWorker()

    fun tryEnqueue(task: Runnable): Boolean = admitted(task, accepting && queue.offer(task))

    /** A free slot is taken at once, even by a caller with a pending interrupt; only a full lane makes the caller wait. */
    fun enqueueWaiting(task: Runnable, timeout: Duration): Boolean =
        admitted(task, accepting && (queue.offer(task) || offerWaiting(task, timeout)))

    fun queued(): Int = queue.size

    fun stopAccepting() {
        accepting = false
    }

    /**
     * The shutdown cut-off: the lane accepts nothing more and its worker starts no further task (the one it is running
     * finishes). Call [takeRemaining] after it for what was still queued. A task the worker polled just as the lane
     * halted is not in that list: the worker gives it up itself, as CHANNEL_UNAVAILABLE.
     */
    fun halt() {
        accepting = false
        halted = true
    }

    /** Waits until the worker has left (its queue drained, or the lane halted) or [deadlineNanos]; follows a restarted worker. */
    fun awaitDrained(deadlineNanos: Long) {
        try {
            while (true) {
                val current = worker
                if (current === Thread.currentThread()) return // close() called from this lane's own task: never wait for ourselves
                current.join(TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()).coerceAtLeast(1))
                if (current === worker || deadlineNanos - System.nanoTime() <= 0) return
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /** Takes everything still queued out of the lane (shutdown after its deadline, or a worker that died for good). */
    fun takeRemaining(): List<Runnable> = ArrayList<Runnable>().also { queue.drainTo(it) }

    /** A close that raced the offer may have let the worker exit already: take the task back so the caller reports it. */
    private fun admitted(task: Runnable, offered: Boolean): Boolean = offered && (accepting || !queue.remove(task))

    private fun offerWaiting(task: Runnable, timeout: Duration): Boolean =
        try {
            queue.offer(task, timeout.toNanos(), TimeUnit.NANOSECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt() // the caller keeps its interrupt status; the record is reported QUEUE_FULL
            false
        }

    private fun startWorker(): Thread = Thread(::work, name).apply { isDaemon = true; start() }

    private fun work() {
        try {
            loop()
        } catch (died: Throwable) {
            workerDied(died)
        }
    }

    private fun loop() {
        while (!halted && (accepting || queue.isNotEmpty())) {
            val task = try {
                queue.poll(100, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                null
            } ?: continue
            if (halted) return refuse(task) // the lane halted while the worker was polling: this task was still queued then
            try {
                task.run()
            } catch (e: Throwable) {
                // tasks guard themselves; this keeps the lane alive
                logger.error("kafka_publisher_lane_task_failed lane={}: {}", name, e.toString())
            }
        }
    }

    /** A task polled just as the lane halted is failed with the rest, not sent: it would go to a producer that is about to close. */
    private fun refuse(task: Runnable) {
        if (task is LaneTask) quietly { task.abandon(CHANNEL_UNAVAILABLE, null) }
    }

    /**
     * The worker left its loop on an unexpected Throwable. It is restarted once; if it dies again, or no thread can be
     * started, the lane stops accepting and gives its queued records up as INTERNAL instead of stranding them.
     */
    private fun workerDied(cause: Throwable) {
        if (restarted.compareAndSet(false, true)) {
            val replacement = try {
                startWorker()
            } catch (e: Throwable) {
                null
            }
            if (replacement != null) {
                worker = replacement
                quietly { logger.error("kafka_publisher_lane_restarted lane={} cause={}", name, describe(cause)) }
                return
            }
        }
        accepting = false
        val stranded = takeRemaining().filterIsInstance<LaneTask>()
        quietly { logger.error("kafka_publisher_lane_dead lane={} abandoned={} cause={}", name, stranded.size, describe(cause)) }
        for (task in stranded) quietly { task.abandon(INTERNAL, cause) }
    }

    private inline fun quietly(block: () -> Unit) {
        try {
            block()
        } catch (ignored: Throwable) {
            // the lane is already in trouble; nothing left to report to
        }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(PublishLane::class.java)
    }
}
