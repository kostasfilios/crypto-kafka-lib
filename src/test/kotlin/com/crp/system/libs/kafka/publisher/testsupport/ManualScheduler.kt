package com.crp.system.libs.kafka.publisher.testsupport

import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.Callable
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Delayed
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/** A maintenance scheduler the test drives: scheduled tasks wait until the test runs them; delays are recorded. */
class ManualScheduler : AbstractExecutorService(), ScheduledExecutorService {
    class Task(val delayMillis: Long, val runnable: Runnable) : ScheduledFuture<Any?> {
        @Volatile private var cancelled = false
        override fun getDelay(unit: TimeUnit): Long = unit.convert(delayMillis, TimeUnit.MILLISECONDS)
        override fun compareTo(other: Delayed): Int = getDelay(TimeUnit.MILLISECONDS).compareTo(other.getDelay(TimeUnit.MILLISECONDS))
        override fun cancel(mayInterruptIfRunning: Boolean): Boolean = (!cancelled).also { cancelled = true }
        override fun isCancelled(): Boolean = cancelled
        override fun isDone(): Boolean = cancelled
        override fun get(): Any? = null
        override fun get(timeout: Long, unit: TimeUnit): Any? = null
    }

    private val pending = LinkedBlockingQueue<Task>()
    val delays = CopyOnWriteArrayList<Long>()
    val executed = CopyOnWriteArrayList<Runnable>()
    @Volatile var rejecting = false
    @Volatile private var shutdown = false

    /** Runs inside `schedule`, before it accepts or rejects: lets a test interleave a close or a timer deterministically. */
    @Volatile var onSchedule: ((Task) -> Unit)? = null

    override fun schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        val task = Task(unit.toMillis(delay), command)
        onSchedule?.invoke(task)
        if (rejecting || shutdown) throw RejectedExecutionException("maintenance scheduler is shut down")
        delays += task.delayMillis
        pending += task
        return task
    }

    /** Waits for the next scheduled task and returns it without running it. */
    fun awaitScheduled(): Task = pending.poll(WAIT_SECONDS, TimeUnit.SECONDS) ?: error("nothing scheduled within $WAIT_SECONDS s")

    /** Waits for the next scheduled task and runs it on the calling thread (as the maintenance thread would). */
    fun runNext(): Task = awaitScheduled().also { if (!it.isCancelled) it.runnable.run() }

    fun pendingCount(): Int = pending.size

    override fun execute(command: Runnable) {
        if (rejecting || shutdown) throw RejectedExecutionException("maintenance scheduler is shut down")
        executed += command
        command.run()
    }

    override fun shutdown() {
        shutdown = true
    }

    override fun shutdownNow(): MutableList<Runnable> {
        shutdown = true
        return mutableListOf()
    }

    override fun isShutdown(): Boolean = shutdown
    override fun isTerminated(): Boolean = shutdown
    override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean = true
    override fun <V> schedule(callable: Callable<V>, delay: Long, unit: TimeUnit): ScheduledFuture<V> = throw UnsupportedOperationException()
    override fun scheduleAtFixedRate(command: Runnable, initialDelay: Long, period: Long, unit: TimeUnit): ScheduledFuture<*> =
        throw UnsupportedOperationException()
    override fun scheduleWithFixedDelay(command: Runnable, initialDelay: Long, delay: Long, unit: TimeUnit): ScheduledFuture<*> =
        throw UnsupportedOperationException()
}
