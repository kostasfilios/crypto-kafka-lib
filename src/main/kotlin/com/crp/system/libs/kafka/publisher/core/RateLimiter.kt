package com.crp.system.libs.kafka.publisher.core

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

/**
 * Lets the first event per key through in each interval and counts the rest for [flush]: the way failures are logged
 * (first line, then a summary with the count). Monotonic time, so a wall-clock step can neither silence nor repeat it.
 */
internal class RateLimiter<K : Any>(interval: Duration, private val nanoTime: () -> Long) {
    private val intervalNanos = interval.saturatedNanos()

    private class Slot(start: Long) {
        val lastLetThrough = AtomicLong(start)
        val held = LongAdder()
    }

    private val slots = ConcurrentHashMap<K, Slot>()

    /** True for the first event per [key] in each interval; false for the rest, which are counted for [flush]. */
    fun admit(key: K): Boolean {
        val now = nanoTime()
        val slot = slots.computeIfAbsent(key) { Slot(now - intervalNanos) }
        val previous = slot.lastLetThrough.get()
        if (now - previous >= intervalNanos && slot.lastLetThrough.compareAndSet(previous, now)) return true
        slot.held.increment()
        return false
    }

    /** Counts [key] without letting it through (events that are only ever summarised). */
    fun hold(key: K) {
        slots.computeIfAbsent(key) { Slot(nanoTime() - intervalNanos) }.held.increment()
    }

    /** Reports, then resets, what was held back per key since the last flush. */
    fun flush(report: (key: K, count: Long) -> Unit) = slots.forEach { (key, slot) ->
        val count = slot.held.sumThenReset()
        if (count > 0) report(key, count)
    }
}
