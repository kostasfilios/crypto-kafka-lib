package com.crp.system.libs.kafka.publisher.adapters

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.SEND_REJECTED
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.TimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class LoggingPublishFailureHandlerTest {
    private val clock = MutableClock(Instant.parse("2026-10-05T12:00:00Z"))
    private val handler = LoggingPublishFailureHandler(Duration.ofSeconds(10), clock)
    private val logs = LogCapture(LoggingPublishFailureHandler::class.java)
    private val secret = """{"player_id":"42","email":"player@example.com","note":"PAYLOAD-SECRET"}"""

    @AfterEach
    fun detach() = logs.close()

    private fun failure(
        stage: PublishFailureStage = DELIVERY_FAILED,
        topic: String = "t1",
        channel: String = "reporting",
        cause: Throwable? = TimeoutException("Topic t1 not present in metadata after 2000 ms."),
    ) = PublishFailure(channel, topic, "42", stage, 1, true, cause, secret, clock.instant())

    private fun errors() = logs.lines(Level.ERROR)
    private fun summaries() = logs.lines(Level.WARN, "kafka_publish_failed_summary")

    @Test
    fun `logs the first failure with its context and cause chain, never the payload`() {
        handler.onFailure(failure(cause = KafkaException("send failed", TimeoutException("Topic t1 not present in metadata after 2000 ms."))))

        assertThat(errors()).containsExactly(
            "kafka_publish_failed channel=reporting topic=t1 key=42 stage=DELIVERY_FAILED attempt=1 retriable=true " +
                "cause=KafkaException: send failed <- TimeoutException: Topic t1 not present in metadata after 2000 ms.",
        )
    }

    @Test
    fun `never logs the payload, in lines, arguments or summaries`() {
        repeat(5) { handler.onFailure(failure()) }
        repeat(3) { handler.onFailure(failure(stage = QUEUE_FULL)) }
        handler.onFailure(failure(stage = SEND_REJECTED, cause = null))
        handler.flushSummaries()

        assertThat(logs.events).isNotEmpty
        logs.events.forEach { event ->
            assertThat(event.formattedMessage).doesNotContain("PAYLOAD-SECRET").doesNotContain("player@example.com")
            assertThat(event.argumentArray.orEmpty().map { it.toString() }).noneMatch { it.contains("PAYLOAD-SECRET") }
            assertThat(event.throwableProxy).isNull()
        }
    }

    @Test
    fun `rate-limits per channel, topic and stage within the summary interval`() {
        repeat(3) { handler.onFailure(failure()) }
        handler.onFailure(failure(stage = SEND_REJECTED))
        handler.onFailure(failure(topic = "t2"))
        handler.onFailure(failure(channel = "audit"))

        assertThat(errors()).hasSize(4)
        assertThat(errors().filter { it.contains("channel=reporting topic=t1 key=42 stage=DELIVERY_FAILED") }).hasSize(1)
    }

    @Test
    fun `logs again once the interval has passed, not a millisecond before`() {
        handler.onFailure(failure())
        clock.advance(Duration.ofMillis(9_999))
        handler.onFailure(failure())
        assertThat(errors()).hasSize(1)

        clock.advance(Duration.ofMillis(1))
        handler.onFailure(failure())
        assertThat(errors()).hasSize(2)
    }

    /** Releases [parties] threads from `millis()` at the same instant (spinning, not parking), so they race into the CAS. */
    private class RacingClock(private val parties: Int) : Clock() {
        @Volatile var now: Long = 0
        @Volatile private var armed = false
        @Volatile private var go = false
        private val arrived = AtomicInteger()

        fun arm() {
            arrived.set(0)
            go = false
            armed = true
        }

        fun disarm() {
            armed = false
        }

        override fun millis(): Long {
            if (armed) {
                if (arrived.incrementAndGet() == parties) go = true
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
                while (!go && System.nanoTime() < deadline) Thread.onSpinWait()
            }
            return now
        }

        override fun instant(): Instant = Instant.ofEpochMilli(now)
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId?): Clock = this
    }

    @Test
    fun `threads racing for the same due log line log it exactly once and count the rest`() {
        val parties = 4
        val rounds = 200
        val clock = RacingClock(parties)
        val racing = LoggingPublishFailureHandler(Duration.ofSeconds(10), clock)
        val pool = Executors.newFixedThreadPool(parties)
        try {
            repeat(rounds) { round ->
                val topic = "race-$round"
                clock.now = 1_000_000L
                racing.onFailure(failure(topic = topic)) // logged; the key exists, so the racers take the lock-free path
                clock.now += 10_000L // the next line for this key is due
                clock.arm()
                (1..parties).map { pool.submit { racing.onFailure(failure(topic = topic)) } }.forEach { it.get(10, TimeUnit.SECONDS) }
                clock.disarm()
            }
        } finally {
            pool.shutdownNow()
        }
        val perTopic = errors().groupingBy { Regex("topic=(race-\\d+)").find(it)!!.groupValues[1] }.eachCount()
        assertThat(perTopic).hasSize(rounds)
        assertThat(perTopic.values).containsOnly(2) // the warm-up line and exactly one of the racers
        racing.flushSummaries()
        assertThat(summaries()).hasSize(rounds).allMatch { it.contains("count=${parties - 1} ") }
    }

    @Test
    fun `flushes one summary per key with the suppressed count, then resets`() {
        repeat(4) { handler.onFailure(failure()) } // 1 logged + 3 counted
        repeat(2) { handler.onFailure(failure(topic = "t2")) } // 1 logged + 1 counted
        handler.onFailure(failure(stage = SEND_REJECTED)) // logged, nothing counted

        handler.flushSummaries()
        assertThat(summaries()).containsExactlyInAnyOrder(
            "kafka_publish_failed_summary channel=reporting topic=t1 stage=DELIVERY_FAILED count=3 interval=PT10S",
            "kafka_publish_failed_summary channel=reporting topic=t2 stage=DELIVERY_FAILED count=1 interval=PT10S",
        )

        handler.flushSummaries()
        assertThat(summaries()).hasSize(2) // counters were reset
    }

    @Test
    fun `drops are only counted, then summarised`() {
        repeat(25) { handler.onFailure(failure(stage = QUEUE_FULL)) }
        assertThat(errors()).isEmpty()
        handler.flushSummaries()
        assertThat(summaries()).containsExactly("kafka_publish_failed_summary channel=reporting topic=t1 stage=QUEUE_FULL count=25 interval=PT10S")
    }

    @Test
    fun `CHANNEL_UNAVAILABLE is neither logged nor counted`() {
        repeat(3) { handler.onFailure(failure(stage = CHANNEL_UNAVAILABLE)) }
        handler.flushSummaries()
        assertThat(logs.events).isEmpty()
    }

    @Test
    fun `the cause chain stops after five links`() {
        val deep = (1..7).fold(TimeoutException("root") as Throwable) { inner, i -> RuntimeException("w$i", inner) }
        handler.onFailure(failure(cause = deep))
        assertThat(errors().single()).endsWith("cause=RuntimeException: w7 <- RuntimeException: w6 <- RuntimeException: w5 <- RuntimeException: w4 <- RuntimeException: w3")
    }

    @Test
    fun `works from the epoch clock too (the first failure is always logged)`() {
        val epochHandler = LoggingPublishFailureHandler(Duration.ofSeconds(10), MutableClock(Instant.EPOCH))
        epochHandler.onFailure(failure())
        assertThat(errors()).hasSize(1)
    }
}
