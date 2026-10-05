package com.crp.system.libs.kafka.publisher.testsupport

import com.crp.system.libs.kafka.publisher.adapters.ExponentialRetryPolicy
import com.crp.system.libs.kafka.publisher.adapters.GsonEventSerializer
import com.crp.system.libs.kafka.publisher.adapters.KafkaRetriableClassifier
import com.crp.system.libs.kafka.publisher.api.EventSerializer
import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureClassifier
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.api.RetryPolicy
import com.crp.system.libs.kafka.publisher.core.BackpressurePolicy
import com.crp.system.libs.kafka.publisher.core.BlockWithTimeout
import com.crp.system.libs.kafka.publisher.core.CallerRunsWhenFull
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import com.crp.system.libs.kafka.publisher.core.DropWhenFull
import com.crp.system.libs.kafka.publisher.core.FailureDispatcher
import com.crp.system.libs.kafka.publisher.core.PublishLanes
import com.crp.system.libs.kafka.publisher.spring.BackpressureMode
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

data class SampleEvent(val eventType: String, val playerId: String, val seq: Int = 0)

/** One ChannelEventPublisher over fakes: a scripted sender, a manual maintenance scheduler and a hand-moved clock. */
internal class Harness(
    val settings: ChannelSettings = ChannelSettings(enabled = true, lanes = 1, queueCapacity = 16),
    val sender: FakeRecordSender = FakeRecordSender(),
    val clock: MutableClock = MutableClock(),
    val scheduler: ManualScheduler = ManualScheduler(),
    serializer: EventSerializer = GsonEventSerializer(settings.serializer),
    backpressure: BackpressurePolicy? = null,
    classifier: PublishFailureClassifier = KafkaRetriableClassifier,
    retryPolicy: RetryPolicy = ExponentialRetryPolicy(settings.retry),
    extraHandlers: List<PublishFailureHandler> = emptyList(),
    val channel: String = "ch${counter.incrementAndGet()}",
    metricsOverride: PublisherMetrics? = null,
) : AutoCloseable {
    val handler = RecordingFailureHandler()
    val metrics = RecordingMetrics()
    private val effectiveMetrics: PublisherMetrics = metricsOverride ?: metrics
    val lanes = PublishLanes(channel, settings.lanes, settings.queueCapacity, settings.ordering)
    val publisher = ChannelEventPublisher(
        channel = channel,
        settings = settings,
        serializer = serializer,
        sender = sender,
        lanes = lanes,
        backpressure = backpressure ?: when (settings.backpressure) {
            BackpressureMode.DROP -> DropWhenFull
            BackpressureMode.CALLER_RUNS -> CallerRunsWhenFull
            BackpressureMode.BLOCK_WITH_TIMEOUT -> BlockWithTimeout(settings.blockTimeout)
        },
        classifier = classifier,
        retryPolicy = retryPolicy,
        maintenance = scheduler,
        failures = FailureDispatcher(channel, listOf(handler) + extraHandlers, effectiveMetrics, clock),
        metrics = effectiveMetrics,
        clock = clock,
    )

    override fun close() = publisher.close()

    companion object {
        private val counter = AtomicInteger()
    }
}

fun <T> CompletableFuture<T>.awaitResult(): T = get(WAIT_SECONDS, TimeUnit.SECONDS)

/** The failure a publishWithResult future completed with. */
fun CompletableFuture<PublishResult>.awaitFailure(): PublishFailure =
    try {
        get(WAIT_SECONDS, TimeUnit.SECONDS)
        throw AssertionError("the future completed normally")
    } catch (e: ExecutionException) {
        (e.cause as? PublishFailedException)?.failure ?: throw AssertionError("unexpected cause", e.cause)
    }
