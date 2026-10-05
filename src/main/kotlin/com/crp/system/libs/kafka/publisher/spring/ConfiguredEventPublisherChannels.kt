package com.crp.system.libs.kafka.publisher.spring

import com.crp.system.libs.kafka.publisher.adapters.AdminClientTopicInspector
import com.crp.system.libs.kafka.publisher.adapters.ChannelProducerConfig
import com.crp.system.libs.kafka.publisher.adapters.ExponentialRetryPolicy
import com.crp.system.libs.kafka.publisher.adapters.GsonEventSerializer
import com.crp.system.libs.kafka.publisher.adapters.KafkaRecordSender
import com.crp.system.libs.kafka.publisher.adapters.KafkaRetriableClassifier
import com.crp.system.libs.kafka.publisher.adapters.LoggingPublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.EventPublisher
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.core.BackpressurePolicy
import com.crp.system.libs.kafka.publisher.core.BlockWithTimeout
import com.crp.system.libs.kafka.publisher.core.CallerRunsWhenFull
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import com.crp.system.libs.kafka.publisher.core.DisabledEventPublisher
import com.crp.system.libs.kafka.publisher.core.DropWhenFull
import com.crp.system.libs.kafka.publisher.core.FailureDispatcher
import com.crp.system.libs.kafka.publisher.core.PublishLanes
import com.crp.system.libs.kafka.publisher.core.RecordSender
import com.crp.system.libs.kafka.publisher.core.TopicInspector
import com.crp.system.libs.kafka.publisher.core.saturatedNanos
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The channel catalog. Each configured channel is either a [ChannelEventPublisher] (its own producer, lanes and
 * policies) or a [DisabledEventPublisher]: disabled, no unique KafkaTemplate, or invalid settings (AD10: an invalid
 * channel is switched off with an ERROR, it never stops the service).
 */
internal class ConfiguredEventPublisherChannels private constructor(
    private val publishers: Map<String, EventPublisher>,
    private val active: List<ChannelEventPublisher>,
    private val maintenance: ScheduledExecutorService?,
    private val logging: LoggingPublishFailureHandler,
    private val clock: Clock,
) : EventPublisherChannels, AutoCloseable {

    private val warnedUnknown: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val closed = AtomicBoolean(false)

    override fun get(name: String): EventPublisher =
        publishers[name] ?: DisabledEventPublisher(name, clock).also {
            if (warnedUnknown.add(name)) logger.warn("kafka_publisher_channel_unknown channel={} configured={}", name, publishers.keys)
        }

    override fun names(): Set<String> = publishers.keys

    /**
     * Every channel stops accepting at once, then all drain against one shared deadline (the longest shutdown timeout,
     * so shutdown takes the longest timeout, not the sum); each closes its producer and fails what is still queued.
     * Then the maintenance thread stops and the summaries are flushed once more.
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        val start = System.nanoTime()
        val closing = active.filter { channel -> guarded(channel.channel) { channel.beginClose() } ?: false }
        val deadline = start + (closing.maxOfOrNull { it.shutdownTimeout.saturatedNanos() } ?: 0L).coerceAtLeast(0)
        for (channel in closing) guarded(channel.channel) { channel.finishClose(deadline) }
        maintenance?.let { stopMaintenance(it) }
        flushSummaries(logging, active)
    }

    private inline fun <T> guarded(channel: String, block: () -> T): T? =
        try {
            block()
        } catch (e: Throwable) {
            logger.warn("kafka_publisher_channel_close_failed channel={}: {}", channel, e.toString())
            null
        }

    companion object {
        const val MAINTENANCE_THREAD = "kafka-publisher-maintenance"
        val DEFAULT_SUMMARY_INTERVAL: Duration = Duration.ofSeconds(10)
        const val MAX_LANES = 16
        const val MAX_QUEUE_CAPACITY = 1_000_000
        const val MAX_LANE_CAPACITY = 250_000
        private val TOPIC_CHECK_TIMEOUT: Duration = Duration.ofSeconds(5)
        private val logger = LoggerFactory.getLogger(ConfiguredEventPublisherChannels::class.java)

        fun create(
            properties: EventPublisherProperties,
            sharedProducerConfig: Map<String, Any>?,
            extraFailureHandlers: List<PublishFailureHandler>,
            metrics: PublisherMetrics,
            applicationName: String,
            clock: Clock,
            senderFactory: (Map<String, Any>) -> RecordSender = { config -> KafkaRecordSender(KafkaProducer<String, String>(config)) },
            topicInspectorFactory: (Map<String, Any>) -> TopicInspector = { config -> AdminClientTopicInspector(config) },
            /** Channels whose property values did not convert, with the reason (from the strict re-bind). */
            bindingErrors: Map<String, String> = emptyMap(),
            /** Monotonic time for rate limits, latency and cool-downs. */
            nanoTime: () -> Long = System::nanoTime,
        ): ConfiguredEventPublisherChannels {
            val summaryInterval = validSummaryInterval(properties.summaryInterval)
            val logging = LoggingPublishFailureHandler(summaryInterval, nanoTime)
            val handlers = listOf<PublishFailureHandler>(logging) + extraFailureHandlers
            val publishers = LinkedHashMap<String, EventPublisher>()
            val active = ArrayList<ChannelEventPublisher>()
            var maintenance: ScheduledExecutorService? = null // no thread until it gets work, and only for enabled channels

            for ((name, settings) in properties.channels) {
                publishers[name] = DisabledEventPublisher(name, clock)
                val unbindable = bindingErrors[name]
                if (unbindable != null) {
                    logger.error("kafka_publisher_channel_invalid channel={} reason={}", name, unbindable)
                    continue
                }
                if (!settings.enabled) {
                    logger.info("kafka_publisher_channel_disabled channel={}", name)
                    continue
                }
                if (sharedProducerConfig == null) {
                    logger.error("kafka_publisher_channel_disabled channel={} reason=no_unique_kafka_template", name)
                    continue
                }
                val invalid = invalidReason(settings)
                if (invalid != null) {
                    logger.error("kafka_publisher_channel_invalid channel={} reason={}", name, invalid)
                    continue
                }
                val producerConfig: Map<String, Any>
                val inspector: TopicInspector?
                try { // nothing has started yet
                    producerConfig = ChannelProducerConfig.build(sharedProducerConfig, name, settings.producer, applicationName)
                    inspector = if (settings.topics.isEmpty()) null else topicInspectorFactory(producerConfig)
                } catch (e: Throwable) {
                    logger.error("kafka_publisher_channel_invalid channel={} reason=startup cause={}", name, causeOf(e))
                    continue
                }
                val sender = try {
                    senderFactory(producerConfig)
                } catch (e: Throwable) {
                    logger.error("kafka_publisher_channel_invalid channel={} reason=producer_config cause={}", name, causeOf(e))
                    continue
                }
                var built: ChannelEventPublisher? = null
                try {
                    val executor = maintenance ?: newMaintenance().also { maintenance = it }
                    val lanes = PublishLanes(name, settings.lanes, settings.queueCapacity, settings.ordering)
                    val channel = ChannelEventPublisher(
                        channel = name,
                        settings = settings,
                        serializer = GsonEventSerializer(settings.serializer),
                        sender = sender,
                        lanes = lanes,
                        backpressure = backpressureFor(settings),
                        classifier = KafkaRetriableClassifier,
                        retryPolicy = ExponentialRetryPolicy(settings.retry),
                        maintenance = executor,
                        failures = FailureDispatcher(name, handlers, metrics, clock, summaryInterval, nanoTime),
                        metrics = metrics,
                        clock = clock,
                        nanoTime = nanoTime,
                        warnInterval = summaryInterval,
                    )
                    built = channel
                    publishers[name] = channel
                    active += channel
                    registerQueueDepth(metrics, name, lanes)
                    if (inspector != null) checkTopics(executor, name, settings.topics, inspector)
                    logger.info(
                        "kafka_publisher_channel_enabled channel={} client_id={} bootstrap={} lanes={} queue_capacity={} backpressure={} " +
                            "ordering={} max_attempts={} max_block_ms={} missing_topic_cooldown={}",
                        name, producerConfig[ProducerConfig.CLIENT_ID_CONFIG], producerConfig[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG],
                        settings.lanes, settings.queueCapacity, settings.backpressure, settings.ordering, settings.retry.maxAttempts,
                        producerConfig[ProducerConfig.MAX_BLOCK_MS_CONFIG], settings.producer.missingTopicCooldown,
                    )
                } catch (e: Throwable) {
                    logger.error("kafka_publisher_channel_invalid channel={} reason=startup cause={}", name, causeOf(e))
                    publishers[name] = DisabledEventPublisher(name, clock)
                    if (built != null) {
                        active.remove(built)
                        runCatching { built.close() }
                    } else {
                        runCatching { sender.close(Duration.ZERO) }
                    }
                }
            }
            if (active.isEmpty()) {
                maintenance?.shutdownNow() // a channel that failed at startup leaves no maintenance thread behind
                maintenance = null
            } else {
                maintenance?.scheduleWithFixedDelay(
                    { flushSummaries(logging, active) }, summaryInterval.toMillis(), summaryInterval.toMillis(), TimeUnit.MILLISECONDS,
                )
            }
            return ConfiguredEventPublisherChannels(publishers, active, maintenance, logging, clock)
        }

        /** Null when the settings are usable, otherwise why not. */
        fun invalidReason(settings: ChannelSettings): String? {
            val retry = settings.retry
            val producer = settings.producer
            return when {
                settings.lanes !in 1..MAX_LANES -> "lanes=${settings.lanes} is outside 1-$MAX_LANES"
                settings.queueCapacity < settings.lanes -> "queue-capacity=${settings.queueCapacity} is below lanes=${settings.lanes}"
                settings.queueCapacity > MAX_QUEUE_CAPACITY -> "queue-capacity=${settings.queueCapacity} is above $MAX_QUEUE_CAPACITY"
                perLaneCapacity(settings) > MAX_LANE_CAPACITY ->
                    "queue-capacity=${settings.queueCapacity} gives ${perLaneCapacity(settings)} per lane, above $MAX_LANE_CAPACITY"
                settings.backpressure == BackpressureMode.BLOCK_WITH_TIMEOUT && !positive(settings.blockTimeout) ->
                    "block-timeout=${settings.blockTimeout} must be > 0 under BLOCK_WITH_TIMEOUT"
                retry.maxAttempts < 1 -> "retry.max-attempts=${retry.maxAttempts} is below 1"
                !(retry.multiplier >= 1.0) -> "retry.multiplier=${retry.multiplier} is below 1"
                !positive(retry.initialBackoff) -> "retry.initial-backoff=${retry.initialBackoff} must be > 0"
                !positive(retry.maxBackoff) -> "retry.max-backoff=${retry.maxBackoff} must be > 0"
                retry.maxBackoff < retry.initialBackoff -> "retry.max-backoff=${retry.maxBackoff} is below retry.initial-backoff=${retry.initialBackoff}"
                producer.deliveryTimeoutMs.toLong() < producer.lingerMs.toLong() + producer.requestTimeoutMs ->
                    "producer.delivery-timeout-ms=${producer.deliveryTimeoutMs} is below linger-ms + request-timeout-ms " +
                        "(${producer.lingerMs} + ${producer.requestTimeoutMs})"
                producer.missingTopicCooldown.isNegative -> "producer.missing-topic-cooldown=${producer.missingTopicCooldown} is negative (0 turns it off)"
                else -> null
            }
        }

        /** What each lane's queue gets (the split rounds up). Only meaningful once lanes is in range. */
        private fun perLaneCapacity(settings: ChannelSettings): Int = (settings.queueCapacity + settings.lanes - 1) / settings.lanes

        private fun positive(duration: Duration): Boolean = !duration.isZero && !duration.isNegative

        private fun backpressureFor(settings: ChannelSettings): BackpressurePolicy = when (settings.backpressure) {
            BackpressureMode.DROP -> DropWhenFull
            BackpressureMode.CALLER_RUNS -> CallerRunsWhenFull
            BackpressureMode.BLOCK_WITH_TIMEOUT -> BlockWithTimeout(settings.blockTimeout)
        }

        private fun validSummaryInterval(interval: Duration): Duration {
            if (positive(interval)) return interval
            logger.error("kafka_publisher_invalid_setting summary-interval={} reason=not_positive using={}", interval, DEFAULT_SUMMARY_INTERVAL)
            return DEFAULT_SUMMARY_INTERVAL
        }

        private fun newMaintenance(): ScheduledExecutorService =
            ScheduledThreadPoolExecutor(1, ThreadFactory { task -> Thread(task, MAINTENANCE_THREAD).apply { isDaemon = true } }).apply {
                removeOnCancelPolicy = true
                executeExistingDelayedTasksAfterShutdownPolicy = false
            }

        private fun stopMaintenance(executor: ScheduledExecutorService) {
            executor.shutdown()
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) executor.shutdownNow()
            } catch (e: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            }
        }

        private fun registerQueueDepth(metrics: PublisherMetrics, channel: String, lanes: PublishLanes) {
            try {
                metrics.queueDepth(channel) { lanes.queued() }
            } catch (e: Throwable) {
                logger.warn("kafka_publisher_metrics_failed channel={}: {}", channel, e.toString())
            }
        }

        /** One-off, on the maintenance thread, bounded at 5 s. Never creates a topic. */
        private fun checkTopics(maintenance: ScheduledExecutorService, channel: String, topics: List<String>, inspector: TopicInspector) {
            maintenance.execute {
                try {
                    inspector.missingTopics(topics, TOPIC_CHECK_TIMEOUT).forEach { topic ->
                        logger.error("kafka_publisher_topic_missing channel={} topic={}", channel, topic)
                    }
                } catch (e: Throwable) {
                    logger.warn("kafka_publisher_topic_check_failed channel={} topics={} cause={}", channel, topics, causeOf(e))
                }
            }
        }

        /** The failure summaries, then the WARNs each channel held back (failing handlers, failing metrics). */
        private fun flushSummaries(logging: LoggingPublishFailureHandler, channels: List<ChannelEventPublisher>) {
            try {
                logging.flushSummaries()
                channels.forEach { it.flushWarnings() }
            } catch (e: Throwable) {
                logger.warn("kafka_publisher_summary_failed: {}", e.toString())
            }
        }

        /** The first five links of the cause chain; never throws, even for an exception whose message does. */
        private fun causeOf(error: Throwable): String =
            generateSequence(error) { it.cause }.take(5).joinToString(" <- ") { link ->
                "${link.javaClass.simpleName}: ${runCatching { link.message }.getOrNull()}"
            }
    }
}
