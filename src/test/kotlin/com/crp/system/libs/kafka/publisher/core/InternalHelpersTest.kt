package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class InternalHelpersTest {

    @Test
    fun `saturatedNanos converts, and caps absurd durations instead of overflowing`() {
        assertThat(Duration.ofSeconds(5).saturatedNanos()).isEqualTo(5_000_000_000L)
        assertThat(Duration.ofSeconds(Long.MAX_VALUE).saturatedNanos()).isEqualTo(Long.MAX_VALUE / 4)
        assertThat(Duration.ofSeconds(Long.MIN_VALUE / 2).saturatedNanos()).isEqualTo(Long.MIN_VALUE / 4)
    }

    @Test
    fun `describe never throws, even for an exception that cannot print itself`() {
        val unprintable = object : RuntimeException() {
            override fun toString(): String = throw IllegalStateException("no")
        }
        assertThat(describe(IllegalStateException("plain"))).isEqualTo("java.lang.IllegalStateException: plain")
        assertThat(describe(unprintable)).isEqualTo(unprintable.javaClass.name)
    }

    @Test
    fun `headerSnapshot copies, keeps order, and leaves out null names and values`() {
        @Suppress("UNCHECKED_CAST")
        val headers = linkedMapOf<String?, String?>("b" to "2", null to "x", "a" to "1", "gone" to null) as Map<String, String?>
        assertThat(headerSnapshot(headers)).containsExactly(org.assertj.core.api.Assertions.entry("b", "2"), org.assertj.core.api.Assertions.entry("a", "1"))
        assertThat(headerSnapshot(null)).isEmpty()
        assertThat(headerSnapshot(emptyMap())).isEmpty()
    }

    @Test
    fun `the rate limiter lets the first event through even after only holds, and flushes holds and suppressions`() {
        val clock = MutableClock()
        val limiter = RateLimiter<String>(Duration.ofSeconds(10), clock::nanoTime)
        limiter.hold("k")
        limiter.hold("k")
        assertThat(limiter.admit("k")).isTrue()
        assertThat(limiter.admit("k")).isFalse()

        val flushed = mutableMapOf<String, Long>()
        limiter.flush { key, count -> flushed[key] = count }
        assertThat(flushed).isEqualTo(mapOf("k" to 3L))
        flushed.clear()
        limiter.flush { key, count -> flushed[key] = count }
        assertThat(flushed).isEmpty()

        clock.advance(Duration.ofSeconds(10))
        assertThat(limiter.admit("k")).isTrue()
    }
}
