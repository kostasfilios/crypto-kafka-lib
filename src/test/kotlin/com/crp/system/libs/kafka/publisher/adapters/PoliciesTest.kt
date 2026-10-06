package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.spring.RetrySettings
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.NetworkException
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.ExecutionException

class ExponentialRetryPolicyTest {
    private fun policy(maxAttempts: Int, initial: Long = 200, multiplier: Double = 2.0, max: Long = 5_000) =
        ExponentialRetryPolicy(RetrySettings(maxAttempts, Duration.ofMillis(initial), multiplier, Duration.ofMillis(max)))

    @Test
    fun `max-attempts 1 means no retry`() {
        assertThat(policy(1).delayBeforeAttempt(2)).isNull()
    }

    @Test
    fun `delays grow by the multiplier from the initial backoff`() {
        val p = policy(5)
        assertThat((2..5).map { p.delayBeforeAttempt(it) })
            .containsExactly(Duration.ofMillis(200), Duration.ofMillis(400), Duration.ofMillis(800), Duration.ofMillis(1_600))
    }

    @Test
    fun `the last allowed attempt still gets a delay, the one after it does not`() {
        val p = policy(3)
        assertThat(p.delayBeforeAttempt(3)).isEqualTo(Duration.ofMillis(400))
        assertThat(p.delayBeforeAttempt(4)).isNull()
    }

    @Test
    fun `delays are capped at max-backoff, even for absurd attempt numbers`() {
        val p = policy(maxAttempts = 1_000, initial = 200, multiplier = 10.0, max = 1_000)
        assertThat(p.delayBeforeAttempt(2)).isEqualTo(Duration.ofMillis(200))
        assertThat(p.delayBeforeAttempt(3)).isEqualTo(Duration.ofMillis(1_000))
        assertThat(p.delayBeforeAttempt(999)).isEqualTo(Duration.ofMillis(1_000))
    }

    @Test
    fun `a delay equal to max-backoff is kept`() {
        assertThat(policy(maxAttempts = 3, initial = 500, multiplier = 2.0, max = 1_000).delayBeforeAttempt(3)).isEqualTo(Duration.ofMillis(1_000))
    }

    @Test
    fun `multiplier 1 gives a constant delay`() {
        val p = policy(maxAttempts = 4, initial = 300, multiplier = 1.0)
        assertThat((2..4).map { p.delayBeforeAttempt(it) }).containsOnly(Duration.ofMillis(300))
    }
}

class KafkaRetriableClassifierTest {
    @Test
    fun `Kafka retriable errors are retriable`() {
        listOf(
            TimeoutException("Topic t not present in metadata after 2000 ms."),
            NotEnoughReplicasException("isr"),
            NetworkException("down"),
            NotLeaderOrFollowerException("moved"),
        ).forEach { assertThat(KafkaRetriableClassifier.isRetriable(it)).describedAs(it.javaClass.simpleName).isTrue() }
    }

    @Test
    fun `everything else is final`() {
        listOf(
            RecordTooLargeException("big"),
            TopicAuthorizationException(setOf("t")),
            SerializationException("bad"),
            IllegalStateException("Cannot perform operation after producer has been closed"),
            InterruptException("interrupted"),
            KafkaException("plain"),
        ).forEach { assertThat(KafkaRetriableClassifier.isRetriable(it)).describedAs(it.javaClass.simpleName).isFalse() }
    }

    @Test
    fun `a retriable cause inside wrappers counts`() {
        assertThat(KafkaRetriableClassifier.isRetriable(KafkaException("wrapped", TimeoutException()))).isTrue()
        assertThat(KafkaRetriableClassifier.isRetriable(ExecutionException(RuntimeException(TimeoutException())))).isTrue()
    }

    @Test
    fun `only the first 10 links of the cause chain are looked at`() {
        fun wrapped(depth: Int): Throwable = (1..depth).fold(TimeoutException() as Throwable) { inner, i -> RuntimeException("w$i", inner) }
        assertThat(KafkaRetriableClassifier.isRetriable(wrapped(9))).isTrue() // the 10th link
        assertThat(KafkaRetriableClassifier.isRetriable(wrapped(10))).isFalse() // the 11th link
    }

    @Test
    fun `a cause cycle terminates`() {
        val a = RuntimeException("a")
        val b = RuntimeException("b", a)
        a.initCause(b)
        assertThat(KafkaRetriableClassifier.isRetriable(a)).isFalse()
    }
}
