package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.INTERNAL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.SEND_REJECTED
import com.crp.system.libs.kafka.publisher.api.PublishFailureClassifier
import com.crp.system.libs.kafka.publisher.api.RetryPolicy
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.spring.RetrySettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.Threads
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.TimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class RetryTest {
    private val harnesses = mutableListOf<Harness>()
    private val event = SampleEvent("x", "1")
    private val metadataTimeout = TimeoutException("Topic t1 not present in metadata after 2000 ms.")

    @AfterEach
    fun closeAll() = harnesses.forEach { it.close() }

    private fun harness(
        maxAttempts: Int,
        sender: FakeRecordSender,
        capacity: Int = 16,
        retryPolicy: RetryPolicy? = null,
        shutdownTimeout: Duration = Duration.ofSeconds(5),
    ): Harness {
        val settings = ChannelSettings(
            enabled = true, lanes = 1, queueCapacity = capacity, shutdownTimeout = shutdownTimeout,
            retry = RetrySettings(maxAttempts = maxAttempts, initialBackoff = Duration.ofMillis(200), multiplier = 2.0, maxBackoff = Duration.ofSeconds(5)),
        )
        val h = if (retryPolicy == null) Harness(settings, sender) else Harness(settings, sender, retryPolicy = retryPolicy)
        return h.also { harnesses += it }
    }

    @Test
    fun `retries are off by default`() {
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 4), FakeRecordSender(FakeRecordSender.fail(metadataTimeout)))
            .also { harnesses += it }
        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.retriable).isTrue()
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(h.scheduler.delays).isEmpty()
    }

    @Test
    fun `a non-retriable error is never retried`() {
        val h = harness(maxAttempts = 3, sender = FakeRecordSender(FakeRecordSender.fail(RecordTooLargeException("too large"))))
        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.retriable).isFalse()
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(h.scheduler.delays).isEmpty()
        assertThat(h.sender.calls).hasSize(1)
    }

    @Test
    fun `retriable errors are retried with exponential delays up to max-attempts, then fail with the last attempt`() {
        val h = harness(maxAttempts = 3, sender = FakeRecordSender(FakeRecordSender.fail(metadataTimeout)))
        val result = h.publisher.publishWithResult("t1", "k", event)

        h.scheduler.runNext() // after attempt 1
        h.scheduler.runNext() // after attempt 2
        val failure = result.awaitFailure()

        assertThat(h.scheduler.delays).containsExactly(200L, 400L)
        assertThat(h.sender.calls).hasSize(3)
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.attempt).isEqualTo(3)
        assertThat(failure.retriable).isTrue()
        assertThat(failure.cause).isSameAs(metadataTimeout)
        assertThat(h.handler.failures).hasSize(1) // intermediate attempts are not failures
        assertThat(h.metrics.failed).hasSize(1)
    }

    @Test
    fun `a retry that succeeds completes the future with its attempt number`() {
        val h = harness(
            maxAttempts = 3,
            sender = FakeRecordSender(FakeRecordSender.sequence(FakeRecordSender.fail(NotEnoughReplicasException("isr")), FakeRecordSender.deliver())),
        )
        val result = h.publisher.publishWithResult("t1", "k", event)
        h.scheduler.runNext()

        assertThat(result.awaitResult().attempts).isEqualTo(2)
        assertThat(h.handler.failures).isEmpty()
        assertThat(h.sender.calls.map { it.thread }).allMatch { it == "${h.channel}-publisher-0" } // the retry goes back through the lane
    }

    @Test
    fun `a retriable error wrapped by the producer is still retriable`() {
        val h = harness(maxAttempts = 2, sender = FakeRecordSender(FakeRecordSender.fail(KafkaException("wrapped", metadataTimeout))))
        val result = h.publisher.publishWithResult("t1", "k", event)
        h.scheduler.runNext()
        assertThat(result.awaitFailure().attempt).isEqualTo(2)
    }

    @Test
    fun `a send that throws a retriable error is retried, a non-retriable one is SEND_REJECTED at once`() {
        val retriable = harness(maxAttempts = 2, sender = FakeRecordSender(FakeRecordSender.throwing(metadataTimeout)))
        val first = retriable.publisher.publishWithResult("t1", "k", event)
        retriable.scheduler.runNext()
        val retried = first.awaitFailure()
        assertThat(retried.stage).isEqualTo(SEND_REJECTED)
        assertThat(retried.attempt).isEqualTo(2)

        val rejected = harness(maxAttempts = 2, sender = FakeRecordSender(FakeRecordSender.throwing(IllegalStateException("closed"))))
        val failure = rejected.publisher.publishWithResult("t1", "k", event).awaitFailure()
        assertThat(failure.stage).isEqualTo(SEND_REJECTED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(rejected.scheduler.delays).isEmpty()
    }

    @Test
    fun `a retry finding the lane full is QUEUE_FULL with the failed attempt and the original cause`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            val h = harness(
                maxAttempts = 3,
                capacity = 1,
                sender = FakeRecordSender { record, index, onOutcome ->
                    when (record.key) {
                        "retry-me" -> FakeRecordSender.fail(metadataTimeout)(record, index, onOutcome)
                        "stuck" -> FakeRecordSender.stuck(entered, release)(record, index, onOutcome)
                        else -> FakeRecordSender.deliver()(record, index, onOutcome)
                    }
                },
            )
            val result = h.publisher.publishWithResult("t1", "retry-me", event)
            val retry = h.scheduler.awaitScheduled()
            h.publisher.publish("t1", "stuck", event)
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
            h.publisher.publish("t1", "queued", event)
            eventually { h.lanes.queued() == 1 }

            retry.runnable.run() // the maintenance thread fires the retry

            val failure = result.awaitFailure()
            assertThat(failure.stage).isEqualTo(QUEUE_FULL)
            assertThat(failure.attempt).isEqualTo(1)
            assertThat(failure.cause).isSameAs(metadataTimeout)
            assertThat(failure.retriable).isTrue()
            assertThat(failure.payload).isNotNull()
        } finally {
            release.countDown()
        }
    }

    @Test
    fun `nothing is retried once the channel is closing`() {
        val heldCallback = CompletableFuture<(SendOutcome) -> Unit>()
        val h = harness(maxAttempts = 3, sender = FakeRecordSender { _, _, onOutcome -> heldCallback.complete(onOutcome) })
        val result = h.publisher.publishWithResult("t1", "k", event)
        val callback = heldCallback.awaitResult()

        h.publisher.close()
        callback(SendOutcome.Failed(metadataTimeout)) // the producer reports the failure after close

        val failure = result.awaitFailure()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.retriable).isTrue()
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(h.scheduler.delays).isEmpty()
    }

    @Test
    fun `a pending retry is failed at close with its original stage, and the late timer does nothing`() {
        val h = harness(maxAttempts = 3, sender = FakeRecordSender(FakeRecordSender.fail(metadataTimeout)))
        val result = h.publisher.publishWithResult("t1", "k", event)
        val pending = h.scheduler.awaitScheduled()

        h.publisher.close()

        val failure = result.awaitFailure()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(pending.isCancelled).isTrue()
        pending.runnable.run() // a timer that fires anyway
        assertThat(h.sender.calls).hasSize(1)
        assertThat(h.handler.failures).hasSize(1)
    }

    @Test
    fun `a retry that fires after close reports the original failure without sending`() {
        val h = harness(maxAttempts = 3, sender = FakeRecordSender(FakeRecordSender.fail(metadataTimeout)))
        val result = h.publisher.publishWithResult("t1", "k", event)
        val pending = h.scheduler.awaitScheduled()
        // simulate the timer winning the race against close: it runs while the channel is already closed
        val open = ChannelEventPublisher::class.java.getDeclaredField("open").apply { isAccessible = true }
        (open.get(h.publisher) as java.util.concurrent.atomic.AtomicBoolean).set(false)

        pending.runnable.run()

        val failure = result.awaitFailure()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(h.sender.calls).hasSize(1)
        assertThat(h.handler.failures).hasSize(1)
    }

    @Test
    fun `a retry scheduled just after close swept the pending retries is failed at once, not left to a timer`() {
        lateinit var h: Harness
        // close() runs to completion between the retry's open check and its scheduling
        val closeDuringRetry = RetryPolicy { h.publisher.close(); Duration.ofMillis(200) }
        h = harness(
            maxAttempts = 3,
            sender = FakeRecordSender(FakeRecordSender.fail(metadataTimeout)),
            retryPolicy = closeDuringRetry,
            shutdownTimeout = Duration.ofMillis(50),
        )

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(h.scheduler.awaitScheduled().isCancelled).isTrue()
        assertThat(h.handler.failures).hasSize(1)
    }

    @Test
    fun `a rejected schedule after close already failed the retry reports nothing twice`() {
        val h = harness(maxAttempts = 3, sender = FakeRecordSender(FakeRecordSender.fail(metadataTimeout)), shutdownTimeout = Duration.ofMillis(50))
        h.scheduler.onSchedule = { h.publisher.close() } // close sweeps the pending retry, then the scheduler refuses it
        h.scheduler.rejecting = true

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        // the failure arrives from close()'s sweep; wait until the lane has also handled the rejection
        assertThat(Threads.awaitGone("${h.channel}-publisher-")).isEmpty()
        assertThat(h.sender.closes).hasSize(1)
        assertThat(h.handler.failures).hasSize(1)
        assertThat(h.metrics.failed).hasSize(1)
    }

    @Test
    fun `a retry whose timer already ran is not failed again when close catches up`() {
        val h = harness(
            maxAttempts = 3,
            sender = FakeRecordSender(FakeRecordSender.sequence(FakeRecordSender.fail(metadataTimeout), FakeRecordSender.deliver())),
            shutdownTimeout = Duration.ofMillis(50),
        )
        // the timer fires at once (attempt 2 is queued), then close() runs before retryOrFail's own check
        h.scheduler.onSchedule = { task -> task.runnable.run(); h.publisher.close() }

        val result = h.publisher.publishWithResult("t1", "k", event).awaitResult()

        assertThat(result.attempts).isEqualTo(2) // the queued attempt was drained and delivered
        assertThat(h.handler.failures).isEmpty()
    }

    @Test
    fun `a retry whose admission throws is INTERNAL with the failed attempt`() {
        val defect = IllegalStateException("lane defect on retry")
        var admissions = 0
        val h = Harness(
            ChannelSettings(enabled = true, lanes = 1, queueCapacity = 4, retry = RetrySettings(maxAttempts = 3)),
            FakeRecordSender(FakeRecordSender.fail(metadataTimeout)),
            backpressure = BackpressurePolicy { lane, task -> if (admissions++ == 0) lane.tryEnqueue(task) else throw defect },
        ).also { harnesses += it }
        val result = h.publisher.publishWithResult("t1", "k", event)

        h.scheduler.runNext()

        val failure = result.awaitFailure()
        assertThat(failure.stage).isEqualTo(INTERNAL)
        assertThat(failure.cause).isSameAs(defect)
        assertThat(failure.attempt).isEqualTo(1)
    }

    @Test
    fun `a classifier that rethrows the error is INTERNAL with that error as the cause`() {
        val h = Harness(
            ChannelSettings(enabled = true, lanes = 1, queueCapacity = 4, retry = RetrySettings(maxAttempts = 3)),
            FakeRecordSender(FakeRecordSender.fail(metadataTimeout)),
            classifier = PublishFailureClassifier { throw it },
        ).also { harnesses += it }

        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()

        assertThat(failure.stage).isEqualTo(INTERNAL)
        assertThat(failure.cause).isSameAs(metadataTimeout)
        assertThat(failure.cause!!.suppressed).isEmpty() // an exception cannot suppress itself
    }

    @Test
    fun `retries that ran or were abandoned leave no trace in the pending set`() {
        val h = harness(
            maxAttempts = 3,
            sender = FakeRecordSender(FakeRecordSender.sequence(FakeRecordSender.fail(metadataTimeout), FakeRecordSender.deliver())),
        )
        val result = h.publisher.publishWithResult("t1", "k", event)
        h.scheduler.runNext()
        result.awaitResult()

        val pending = ChannelEventPublisher::class.java.getDeclaredField("pendingRetries").apply { isAccessible = true }
        assertThat(pending.get(h.publisher) as Set<*>).isEmpty()
    }

    @Test
    fun `a scheduler that is shutting down reports the original failure`() {
        val h = harness(maxAttempts = 3, sender = FakeRecordSender(FakeRecordSender.fail(metadataTimeout)))
        h.scheduler.rejecting = true
        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()
        assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
        assertThat(failure.attempt).isEqualTo(1)
        assertThat(failure.retriable).isTrue()
        assertThat(h.handler.failures).hasSize(1)
    }

    @Test
    fun `a throwing retry policy is INTERNAL, never lost`() {
        val h = harness(
            maxAttempts = 3,
            sender = FakeRecordSender(FakeRecordSender.throwing(metadataTimeout)),
            retryPolicy = RetryPolicy { throw ArithmeticException("overflow") },
        )
        val failure = h.publisher.publishWithResult("t1", "k", event).awaitFailure()
        assertThat(failure.stage).isEqualTo(INTERNAL)
        assertThat(failure.cause).isInstanceOf(ArithmeticException::class.java)
        assertThat(failure.cause!!.suppressed).containsExactly(metadataTimeout)
    }
}
