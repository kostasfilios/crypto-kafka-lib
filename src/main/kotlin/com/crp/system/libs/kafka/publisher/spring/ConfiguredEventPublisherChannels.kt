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

    /** Closes each channel (drain, then producer close), stops the maintenance thread and flushes the summaries once more. */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        for (channel in active) {
            try {
                channel.close()
            } catch (e: Throwable) {
                logger.warn("kafka_publisher_channel_close_failed channel={}: {}", channel.channel, e.toString())
            }
        }
        maintenance?.let { stopMaintenance(it) }
        logging.flushSummaries()
    }

    companion object {
        const val MAINTENANCE_THREAD = "kafka-publisher-maintenance"
        val DEFAULT_SUMMARY_INTERVAL: Duration = Duration.ofSeconds(10)
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
        ): ConfiguredEventPublisherChannels {
            val summaryInterval = validSummaryInterval(properties.summaryInterval)
            val logging = LoggingPublishFailureHandler(summaryInterval, clock)
            val handlers = listOf<PublishFailureHandler>(logging) + extraFailureHandlers
            val publishers = LinkedHashMap<String, EventPublisher>()
            val active = ArrayList<ChannelEventPublisher>()
            var maintenance: ScheduledExecutorService? = null

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
                val producerConfig = ChannelProducerConfig.build(sharedProducerConfig, name, settings.producer, applicationName)
                val sender = try {
                    senderFactory(producerConfig)
                } catch (e: Exception) {
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
                        failures = FailureDispatcher(name, handlers, metrics, clock),
                        metrics = metrics,
                        clock = clock,
                    )
                    built = channel
                    publishers[name] = channel
                    active += channel
                    registerQueueDepth(metrics, name, lanes)
                    if (settings.topics.isNotEmpty()) checkTopics(executor, name, settings.topics, topicInspectorFactory(producerConfig))
                    logger.info(
                        "kafka_publisher_channel_enabled channel={} client_id={} lanes={} queue_capacity={} backpressure={} ordering={} max_attempts={} max_block_ms={}",
                        name, producerConfig[ProducerConfig.CLIENT_ID_CONFIG], settings.lanes, settings.queueCapacity, settings.backpressure,
                        settings.ordering, settings.retry.maxAttempts, producerConfig[ProducerConfig.MAX_BLOCK_MS_CONFIG],
                    )
                } catch (e: Exception) {
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
            maintenance?.scheduleWithFixedDelay(
                { flushQuietly(logging) }, summaryInterval.toMillis(), summaryInterval.toMillis(), TimeUnit.MILLISECONDS,
            )
            return ConfiguredEventPublisherChannels(publishers, active, maintenance, logging, clock)
        }

        /** Null when the settings are usable, otherwise why not. */
        fun invalidReason(settings: ChannelSettings): String? {
            val producer = settings.producer
            return when {
                settings.lanes !in 1..16 -> "lanes=${settings.lanes} is outside 1-16"
                settings.queueCapacity < settings.lanes -> "queue-capacity=${settings.queueCapacity} is below lanes=${settings.lanes}"
                settings.backpressure == BackpressureMode.BLOCK_WITH_TIMEOUT && (settings.blockTimeout.isZero || settings.blockTimeout.isNegative) ->
                    "block-timeout=${settings.blockTimeout} must be > 0 under BLOCK_WITH_TIMEOUT"
                settings.retry.maxAttempts < 1 -> "retry.max-attempts=${settings.retry.maxAttempts} is below 1"
                !(settings.retry.multiplier >= 1.0) -> "retry.multiplier=${settings.retry.multiplier} is below 1"
                producer.deliveryTimeoutMs.toLong() < producer.lingerMs.toLong() + producer.requestTimeoutMs ->
                    "producer.delivery-timeout-ms=${producer.deliveryTimeoutMs} is below linger-ms + request-timeout-ms " +
                        "(${producer.lingerMs} + ${producer.requestTimeoutMs})"
                else -> null
            }
        }

        private fun backpressureFor(settings: ChannelSettings): BackpressurePolicy = when (settings.backpressure) {
            BackpressureMode.DROP -> DropWhenFull
            BackpressureMode.CALLER_RUNS -> CallerRunsWhenFull
            BackpressureMode.BLOCK_WITH_TIMEOUT -> BlockWithTimeout(settings.blockTimeout)
        }

        private fun validSummaryInterval(interval: Duration): Duration {
            if (!interval.isZero && !interval.isNegative) return interval
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

        private fun flushQuietly(logging: LoggingPublishFailureHandler) {
            try {
                logging.flushSummaries()
            } catch (e: Throwable) {
                logger.warn("kafka_publisher_summary_failed: {}", e.toString())
            }
        }

        private fun causeOf(error: Throwable): String =
            generateSequence(error) { it.cause }.take(5).joinToString(" <- ") { "${it.javaClass.simpleName}: ${it.message}" }
    }
}
