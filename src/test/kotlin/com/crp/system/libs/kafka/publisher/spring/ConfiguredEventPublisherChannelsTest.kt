package com.crp.system.libs.kafka.publisher.spring

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.adapters.LoggingPublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import com.crp.system.libs.kafka.publisher.core.DisabledEventPublisher
import com.crp.system.libs.kafka.publisher.core.RecordSender
import com.crp.system.libs.kafka.publisher.core.TopicInspector
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import com.crp.system.libs.kafka.publisher.testsupport.RecordingFailureHandler
import com.crp.system.libs.kafka.publisher.testsupport.RecordingMetrics
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.Threads
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.apache.kafka.common.config.ConfigException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ConfiguredEventPublisherChannelsTest {
    private val shared = mapOf<String, Any>("bootstrap.servers" to "127.0.0.1:1")
    private val clock = MutableClock()
    private val sendersCreated = AtomicInteger()
    private val inspectorsCreated = AtomicInteger()
    private val fakeSender = FakeRecordSender()
    private val opened = mutableListOf<ConfiguredEventPublisherChannels>()
    private val logs = LogCapture(ConfiguredEventPublisherChannels::class.java, LoggingPublishFailureHandler::class.java)

    @AfterEach
    fun cleanUp() {
        opened.forEach { it.close() }
        logs.close()
    }

    private val threadPrefixes = listOf("-publisher-", ConfiguredEventPublisherChannels.MAINTENANCE_THREAD, "kafka-producer-network-thread", "kafka-admin-client-thread")

    private fun publisherThreads(): Set<Thread> = Thread.getAllStackTraces().keys.filter { t -> t.isAlive && threadPrefixes.any { t.name.contains(it) } }.toSet()

    /** Threads started since [before] (by identity: threads of earlier tests may still be winding down). */
    private fun startedSince(before: Set<Thread>): List<String> = (publisherThreads() - before).map { it.name }

    private fun create(
        channels: Map<String, ChannelSettings>,
        sharedConfig: Map<String, Any>? = shared,
        summaryInterval: Duration = Duration.ofSeconds(10),
        handlers: List<PublishFailureHandler> = emptyList(),
        metrics: RecordingMetrics = RecordingMetrics(),
        sender: (Map<String, Any>) -> RecordSender = { sendersCreated.incrementAndGet(); fakeSender },
        inspector: (Map<String, Any>) -> TopicInspector = { inspectorsCreated.incrementAndGet(); TopicInspector { _, _ -> emptySet() } },
    ) = ConfiguredEventPublisherChannels.create(
        properties = EventPublisherProperties(summaryInterval, channels),
        sharedProducerConfig = sharedConfig,
        extraFailureHandlers = handlers,
        metrics = metrics,
        applicationName = "lifecycle-test",
        clock = clock,
        senderFactory = sender,
        topicInspectorFactory = inspector,
    ).also { opened += it }

    // ── what starts and what does not ─────────────────────────────────────────────────────────────────

    @Test
    fun `a disabled channel is a no-op publisher with an INFO line, and starts no thread and no producer`() {
        val before = publisherThreads()
        val channels = create(mapOf("reporting" to ChannelSettings(enabled = false)))

        assertThat(channels.get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
        assertThat(logs.lines(Level.INFO)).contains("kafka_publisher_channel_disabled channel=reporting")
        assertThat(startedSince(before)).isEmpty()
        assertThat(sendersCreated.get()).isZero()
        assertThat(inspectorsCreated.get()).isZero()
    }

    @Test
    fun `no channels at all starts nothing`() {
        val before = publisherThreads()
        val channels = create(emptyMap())
        assertThat(channels.names()).isEmpty()
        assertThat(startedSince(before)).isEmpty()
    }

    @Test
    fun `without a unique KafkaTemplate an enabled channel is switched off with an ERROR, nothing starts`() {
        val before = publisherThreads()
        val channels = create(mapOf("reporting" to ChannelSettings(enabled = true)), sharedConfig = null)

        assertThat(channels.get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
        assertThat(logs.lines(Level.ERROR)).containsExactly("kafka_publisher_channel_disabled channel=reporting reason=no_unique_kafka_template")
        assertThat(startedSince(before)).isEmpty()
        assertThat(sendersCreated.get()).isZero()
    }

    @Test
    fun `each invalid setting switches the channel off with an ERROR naming it, and nothing starts`() {
        val invalid = mapOf(
            "lanes=0" to ChannelSettings(enabled = true, lanes = 0),
            "lanes=17" to ChannelSettings(enabled = true, lanes = 17),
            "queue-capacity=3" to ChannelSettings(enabled = true, lanes = 4, queueCapacity = 3),
            "block-timeout=PT0S" to ChannelSettings(enabled = true, backpressure = BackpressureMode.BLOCK_WITH_TIMEOUT, blockTimeout = Duration.ZERO),
            "block-timeout=PT-0.001S" to ChannelSettings(enabled = true, backpressure = BackpressureMode.BLOCK_WITH_TIMEOUT, blockTimeout = Duration.ofMillis(-1)),
            "retry.max-attempts=0" to ChannelSettings(enabled = true, retry = RetrySettings(maxAttempts = 0)),
            "retry.multiplier=0.5" to ChannelSettings(enabled = true, retry = RetrySettings(multiplier = 0.5)),
            "retry.multiplier=NaN" to ChannelSettings(enabled = true, retry = RetrySettings(multiplier = Double.NaN)),
            "producer.delivery-timeout-ms=10004" to ChannelSettings(enabled = true, producer = ProducerSettings(deliveryTimeoutMs = 10_004)),
        )
        val before = publisherThreads()
        val channels = create(invalid.entries.associate { (reason, settings) -> reason to settings })

        invalid.keys.forEach { name ->
            assertThat(channels.get(name)).describedAs(name).isInstanceOf(DisabledEventPublisher::class.java)
            assertThat(logs.lines(Level.ERROR)).describedAs(name)
                .anyMatch { it.startsWith("kafka_publisher_channel_invalid channel=$name reason=${name.substringBefore('=')}") }
        }
        assertThat(startedSince(before)).isEmpty()
        assertThat(sendersCreated.get()).isZero()
    }

    @Test
    fun `the boundary values are valid`() {
        val valid = listOf(
            ChannelSettings(enabled = true, lanes = 1, queueCapacity = 1),
            ChannelSettings(enabled = true, lanes = 16, queueCapacity = 16),
            ChannelSettings(enabled = true, backpressure = BackpressureMode.DROP, blockTimeout = Duration.ZERO),
            ChannelSettings(enabled = true, backpressure = BackpressureMode.BLOCK_WITH_TIMEOUT, blockTimeout = Duration.ofMillis(1)),
            ChannelSettings(enabled = true, retry = RetrySettings(maxAttempts = 1, multiplier = 1.0)),
            ChannelSettings(enabled = true, producer = ProducerSettings(lingerMs = 5, requestTimeoutMs = 10_000, deliveryTimeoutMs = 10_005)),
        )
        valid.forEach { assertThat(ConfiguredEventPublisherChannels.invalidReason(it)).describedAs(it.toString()).isNull() }
    }

    @Test
    fun `huge timeouts do not overflow the delivery-timeout check`() {
        val settings = ChannelSettings(enabled = true, producer = ProducerSettings(lingerMs = Int.MAX_VALUE, requestTimeoutMs = Int.MAX_VALUE, deliveryTimeoutMs = Int.MAX_VALUE))
        assertThat(ConfiguredEventPublisherChannels.invalidReason(settings)).startsWith("producer.delivery-timeout-ms")
    }

    @Test
    fun `a producer the client rejects switches the channel off, nothing else starts`() {
        val before = publisherThreads()
        val channels = create(
            mapOf("reporting" to ChannelSettings(enabled = true)),
            sender = { throw ConfigException("max.in.flight.requests.per.connection must be at most 5 with idempotence") },
        )
        assertThat(channels.get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
        assertThat(logs.lines(Level.ERROR)).anyMatch { it.startsWith("kafka_publisher_channel_invalid channel=reporting reason=producer_config cause=ConfigException") }
        assertThat(startedSince(before)).isEmpty()
    }

    @Test
    fun `an enabled channel gets its lanes, the maintenance thread, its producer and its queue-depth gauge`() {
        val metrics = RecordingMetrics()
        val channels = create(mapOf("reporting" to ChannelSettings(enabled = true, lanes = 3)), metrics = metrics)

        assertThat(channels.get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
        assertThat(Threads.named("reporting-publisher-")).containsExactlyInAnyOrder("reporting-publisher-0", "reporting-publisher-1", "reporting-publisher-2")
        assertThat(Threads.named(ConfiguredEventPublisherChannels.MAINTENANCE_THREAD)).isNotEmpty
        assertThat(sendersCreated.get()).isEqualTo(1)
        assertThat(metrics.gauges.keys).containsExactly("reporting")
        assertThat(metrics.gauges.getValue("reporting")()).isZero()
        assertThat(logs.lines(Level.INFO)).anyMatch { it.startsWith("kafka_publisher_channel_enabled channel=reporting client_id=lifecycle-test-reporting lanes=3") }
    }

    @Test
    fun `the channel producer gets the shared connection settings and the channel limits`() {
        val configs = CopyOnWriteArrayList<Map<String, Any>>()
        create(
            mapOf("reporting" to ChannelSettings(enabled = true, producer = ProducerSettings(maxBlockMs = 1_234))),
            sender = { config -> configs += config; fakeSender },
        )
        assertThat(configs.single()).containsEntry("bootstrap.servers", "127.0.0.1:1")
            .containsEntry("max.block.ms", 1_234L)
            .containsEntry("client.id", "lifecycle-test-reporting")
    }

    @Test
    fun `an unknown name gets a no-op publisher and one WARN`() {
        val channels = create(mapOf("reporting" to ChannelSettings(enabled = false)))
        val first = channels.get("audit")
        channels.get("audit")
        assertThat(first).isInstanceOf(DisabledEventPublisher::class.java)
        assertThat(first.channel).isEqualTo("audit")
        assertThat(logs.lines(Level.WARN, "kafka_publisher_channel_unknown")).containsExactly("kafka_publisher_channel_unknown channel=audit configured=[reporting]")
    }

    @Test
    fun `names lists every configured channel, enabled or not`() {
        val channels = create(linkedMapOf("reporting" to ChannelSettings(enabled = true), "audit" to ChannelSettings(enabled = false)))
        assertThat(channels.names()).containsExactly("reporting", "audit")
    }

    // ── the topic check ───────────────────────────────────────────────────────────────────────────────

    @Test
    fun `the topic check runs once on the maintenance thread and logs an ERROR per missing topic`() {
        val checkedOn = CompletableFuture<String>()
        val asked = CopyOnWriteArrayList<Pair<Collection<String>, Duration>>()
        create(
            mapOf("reporting" to ChannelSettings(enabled = true, topics = listOf("account.player.events", "payments.request.events"))),
            inspector = { TopicInspector { topics, timeout -> asked += topics to timeout; checkedOn.complete(Thread.currentThread().name); setOf("payments.request.events") } },
        )
        assertThat(checkedOn.awaitResult()).isEqualTo(ConfiguredEventPublisherChannels.MAINTENANCE_THREAD)
        eventually { logs.lines(Level.ERROR).isNotEmpty() }
        assertThat(asked.single()).isEqualTo(listOf("account.player.events", "payments.request.events") to Duration.ofSeconds(5))
        assertThat(logs.lines(Level.ERROR)).containsExactly("kafka_publisher_topic_missing channel=reporting topic=payments.request.events")
    }

    @Test
    fun `a failed topic check is a WARN, not a missing topic`() {
        create(
            mapOf("reporting" to ChannelSettings(enabled = true, topics = listOf("t1"))),
            inspector = { TopicInspector { _, _ -> throw java.util.concurrent.TimeoutException("broker down") } },
        )
        eventually { logs.lines(Level.WARN, "kafka_publisher_topic_check_failed").isNotEmpty() }
        assertThat(logs.lines(Level.ERROR)).isEmpty()
    }

    @Test
    fun `no topics configured means no check`() {
        create(mapOf("reporting" to ChannelSettings(enabled = true)))
        assertThat(inspectorsCreated.get()).isZero()
    }

    // ── handlers, summaries and close ─────────────────────────────────────────────────────────────────

    @Test
    fun `extra failure handlers run after the built-in logging handler`() {
        val order = CopyOnWriteArrayList<String>()
        val extra = PublishFailureHandler { order += "extra:${it.stage}:logged=${logs.lines(Level.ERROR).any { line -> line.startsWith("kafka_publish_failed ") }}" }
        val channels = create(
            mapOf("reporting" to ChannelSettings(enabled = true)),
            handlers = listOf(extra),
            sender = { FakeRecordSender(FakeRecordSender.fail(org.apache.kafka.common.errors.RecordTooLargeException())) },
        )
        channels.get("reporting").publish("t1", "k", SampleEvent("x", "1"))
        eventually { order.isNotEmpty() }
        assertThat(order).containsExactly("extra:DELIVERY_FAILED:logged=true") // the logging handler had already run
    }

    @Test
    fun `summaries are flushed every summary interval by the maintenance thread`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            val handler = RecordingFailureHandler()
            val channels = create(
                mapOf("reporting" to ChannelSettings(enabled = true, lanes = 1, queueCapacity = 1)),
                summaryInterval = Duration.ofMillis(50),
                handlers = listOf(handler),
                sender = { FakeRecordSender(FakeRecordSender.stuck(entered, release)) },
            )
            val reporting = channels.get("reporting")
            reporting.publish("t1", "a", SampleEvent("x", "1"))
            entered.await(5, TimeUnit.SECONDS)
            repeat(4) { reporting.publish("t1", "b$it", SampleEvent("x", "1")) } // 1 queued, 3 dropped
            assertThat(handler.awaitFailures(3)).allMatch { it.stage == QUEUE_FULL }

            // the periodic flush may split the three drops over several lines; together they count three
            val counted = { logs.lines(Level.WARN, "kafka_publish_failed_summary").sumOf { Regex("count=(\\d+)").find(it)!!.groupValues[1].toInt() } }
            eventually { counted() == 3 }
            assertThat(logs.lines(Level.WARN, "kafka_publish_failed_summary"))
                .allMatch { it.startsWith("kafka_publish_failed_summary channel=reporting topic=t1 stage=QUEUE_FULL count=") && it.endsWith(" interval=PT0.05S") }
        } finally {
            release.countDown()
        }
    }

    @Test
    fun `a summary interval that is not positive falls back to the default instead of failing`() {
        val channels = create(mapOf("reporting" to ChannelSettings(enabled = true)), summaryInterval = Duration.ZERO)
        assertThat(channels.get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
        assertThat(logs.lines(Level.ERROR)).containsExactly("kafka_publisher_invalid_setting summary-interval=PT0S reason=not_positive using=PT10S")
    }

    @Test
    fun `close drains and closes every channel, stops the maintenance thread, flushes the summaries, and is idempotent`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val senders = CopyOnWriteArrayList<FakeRecordSender>()
        val channels = create(
            linkedMapOf(
                "reporting" to ChannelSettings(enabled = true, lanes = 1, queueCapacity = 1),
                "audit" to ChannelSettings(enabled = true, lanes = 2),
            ),
            sender = { FakeRecordSender(FakeRecordSender.stuck(entered, release)).also { senders += it } },
        )
        val reporting = channels.get("reporting")
        reporting.publish("t1", "a", SampleEvent("x", "1"))
        entered.await(5, TimeUnit.SECONDS)
        reporting.publish("t1", "queued", SampleEvent("x", "1"))
        reporting.publish("t1", "dropped", SampleEvent("x", "1"))
        release.countDown()

        channels.close()
        channels.close()

        assertThat(senders).hasSize(2).allMatch { it.closes.size == 1 }
        assertThat(senders[0].callsBeforeClose).isEqualTo(2) // drained before the producer closed
        assertThat(Threads.awaitGone("reporting-publisher-")).isEmpty()
        assertThat(Threads.awaitGone("audit-publisher-")).isEmpty()
        assertThat(logs.lines(Level.WARN, "kafka_publish_failed_summary"))
            .containsExactly("kafka_publish_failed_summary channel=reporting topic=t1 stage=QUEUE_FULL count=1 interval=PT10S")
    }

    @Test
    fun `publishing through a closed catalog is CHANNEL_UNAVAILABLE, not an exception`() {
        val channels = create(mapOf("reporting" to ChannelSettings(enabled = true)))
        channels.close()
        val failure = runCatching { channels.get("reporting").publishWithResult("t1", "k", SampleEvent("x", "1")).get(5, TimeUnit.SECONDS) }
        assertThat(failure.exceptionOrNull()?.cause).hasMessageContaining("stage=CHANNEL_UNAVAILABLE")
    }
}
