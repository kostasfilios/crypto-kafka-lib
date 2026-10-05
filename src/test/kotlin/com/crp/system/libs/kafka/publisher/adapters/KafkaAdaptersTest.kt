package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.core.OutboundRecord
import com.crp.system.libs.kafka.publisher.core.SendOutcome
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.spring.EventPublisherProperties
import com.crp.system.libs.kafka.publisher.spring.ProducerSettings
import com.crp.system.libs.kafka.publisher.spring.ConfiguredEventPublisherChannels
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import com.crp.system.libs.kafka.publisher.testsupport.RecordingFailureHandler
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.ServerSocket
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class KafkaRecordSenderTest {
    private val record = OutboundRecord("reporting", "t1", "42", """{"a":1}""", mapOf("source_service" to "KYCService", "trace" to "αβγ"), Instant.EPOCH)

    @Test
    fun `sends a keyed record with UTF-8 headers and reports the acknowledgement`() {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        val outcome = CompletableFuture<SendOutcome>()

        KafkaRecordSender(producer).send(record) { outcome.complete(it) }

        val sent = producer.history().single()
        assertThat(sent.topic()).isEqualTo("t1")
        assertThat(sent.key()).isEqualTo("42")
        assertThat(sent.value()).isEqualTo("""{"a":1}""")
        assertThat(sent.partition() == null).describedAs("no explicit partition: the producer's partitioner decides by key").isTrue()
        assertThat(sent.headers().map { it.key() to String(it.value(), Charsets.UTF_8) })
            .containsExactlyInAnyOrder("source_service" to "KYCService", "trace" to "αβγ")
        assertThat(outcome.awaitResult()).isInstanceOf(SendOutcome.Delivered::class.java)
    }

    @Test
    fun `passes the broker's partition and offset through`() {
        val producer = MockProducer(false, StringSerializer(), StringSerializer())
        val outcome = CompletableFuture<SendOutcome>()
        KafkaRecordSender(producer).send(record) { outcome.complete(it) }
        KafkaRecordSender(producer).send(record) {}
        producer.completeNext()
        assertThat(outcome.awaitResult()).isEqualTo(SendOutcome.Delivered(partition = 0, offset = 0))
    }

    @Test
    fun `reports a callback error as Failed`() {
        val producer = MockProducer(false, StringSerializer(), StringSerializer())
        val outcome = CompletableFuture<SendOutcome>()
        val error = TimeoutException("expired")
        KafkaRecordSender(producer).send(record) { outcome.complete(it) }
        producer.errorNext(error)
        assertThat(outcome.awaitResult()).isEqualTo(SendOutcome.Failed(error))
    }

    @Test
    fun `send on a closed producer throws (SEND_REJECTED upstream)`() {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        val sender = KafkaRecordSender(producer)
        sender.close(Duration.ofMillis(10))
        assertThat(producer.closed()).isTrue()
        assertThrows<IllegalStateException> { sender.send(record) {} }
    }

    @Test
    fun `a record without headers sends none`() {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        KafkaRecordSender(producer).send(record.copy(headers = emptyMap(), key = null)) {}
        assertThat(producer.history().single().headers().toArray()).isEmpty()
        assertThat(producer.history().single().key()).isNull()
    }
}

/**
 * The proposal's "missing topic" case against a real KafkaProducer: no broker answers, so the topic never appears in
 * metadata and the producer reports it through the callback after `max-block-ms` (2000 ms by default) as DELIVERY_FAILED.
 * The caller returns at once; the wait happens on the lane thread.
 */
class RealProducerMissingTopicTest {
    @Test
    fun `a topic missing from metadata costs the caller nothing and ends as DELIVERY_FAILED after max-block-ms`() {
        val closedPort = ServerSocket(0).use { it.localPort }
        val handler = RecordingFailureHandler()
        val channels = ConfiguredEventPublisherChannels.create(
            properties = EventPublisherProperties(channels = mapOf("reporting" to ChannelSettings(enabled = true))),
            sharedProducerConfig = mapOf("bootstrap.servers" to "127.0.0.1:$closedPort"),
            extraFailureHandlers = listOf(handler),
            metrics = PublisherMetrics.None,
            applicationName = "missing-topic-test",
            clock = Clock.systemUTC(),
        )
        try {
            val started = System.nanoTime()
            val result = channels.get("reporting").publishWithResult("event-publisher-test-missing", "42", SampleEvent("x", "42"))
            val callerMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

            val failure = result.awaitFailure()
            val totalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

            assertThat(callerMillis).isLessThan(500)
            assertThat(totalMillis).isGreaterThanOrEqualTo(1_900)
            assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
            assertThat(failure.attempt).isEqualTo(1)
            assertThat(failure.retriable).isTrue()
            assertThat(failure.cause).isInstanceOf(TimeoutException::class.java)
                .hasMessageContaining("not present in metadata after 2000 ms")
            assertThat(handler.failures.single().stage).isEqualTo(DELIVERY_FAILED)
        } finally {
            channels.close()
        }
    }
}

class AdminClientTopicInspectorTest {
    @Test
    fun `the admin client gets only admin keys, a check client id and the check timeout`() {
        val producerConfig = ChannelProducerConfig.build(
            mapOf("bootstrap.servers" to "kafka:9092", "security.protocol" to "SSL", "ssl.truststore.location" to "/ts.jks"),
            "reporting", ProducerSettings(), "KYCService",
        )
        val admin = AdminClientTopicInspector.adminConfig(producerConfig, Duration.ofSeconds(5))

        assertThat(admin).containsEntry("bootstrap.servers", "kafka:9092")
            .containsEntry("security.protocol", "SSL")
            .containsEntry("ssl.truststore.location", "/ts.jks")
            .containsEntry(AdminClientConfig.CLIENT_ID_CONFIG, "KYCService-reporting-topic-check")
            .containsEntry(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000)
            .containsEntry(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000)
        assertThat(admin.keys).noneMatch { it in setOf("key.serializer", "value.serializer", "acks", "linger.ms", "max.block.ms", "buffer.memory", "enable.idempotence") }
        assertThat(admin.keys).allMatch { it in AdminClientConfig.configNames() }
    }

    @Test
    fun `no topics means no admin client at all`() {
        assertThat(AdminClientTopicInspector(mapOf("bootstrap.servers" to "127.0.0.1:1")).missingTopics(emptyList(), Duration.ofSeconds(1))).isEmpty()
    }

    @Test
    fun `an unreachable broker makes the check throw within its bound, never report topics missing`() {
        val closedPort = ServerSocket(0).use { it.localPort }
        val inspector = AdminClientTopicInspector(mapOf("bootstrap.servers" to "127.0.0.1:$closedPort", "client.id" to "inspector-test"))
        val started = System.nanoTime()
        assertThrows<Exception> { inspector.missingTopics(listOf("t1"), Duration.ofMillis(1_500)) }
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(8_000)
    }
}

class MicrometerPublisherMetricsTest {
    private val registry = SimpleMeterRegistry()
    private val metrics = MicrometerPublisherMetrics(registry)

    @Test
    fun `delivered records a timer per channel and topic`() {
        metrics.delivered("reporting", "t1", Duration.ofMillis(30))
        metrics.delivered("reporting", "t1", Duration.ofMillis(10))
        metrics.delivered("reporting", "t2", Duration.ofMillis(5))

        val t1 = registry.get("kafka.publisher.delivered").tags("channel", "reporting", "topic", "t1").timer()
        assertThat(t1.count()).isEqualTo(2)
        assertThat(t1.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(40.0)
        assertThat(registry.get("kafka.publisher.delivered").tags("topic", "t2").timer().count()).isEqualTo(1)
    }

    @Test
    fun `failed counts per channel, topic and stage`() {
        repeat(3) { metrics.failed("reporting", "t1", DELIVERY_FAILED) }
        metrics.failed("reporting", "t1", com.crp.system.libs.kafka.publisher.api.PublishFailureStage.QUEUE_FULL)
        metrics.failed("audit", "t1", DELIVERY_FAILED)

        assertThat(registry.get("kafka.publisher.failed").tags("channel", "reporting", "topic", "t1", "stage", "DELIVERY_FAILED").counter().count()).isEqualTo(3.0)
        assertThat(registry.get("kafka.publisher.failed").tags("channel", "reporting", "stage", "QUEUE_FULL").counter().count()).isEqualTo(1.0)
        assertThat(registry.get("kafka.publisher.failed").tags("channel", "audit").counter().count()).isEqualTo(1.0)
    }

    @Test
    fun `the queue depth gauge reads the lanes live`() {
        var depth = 3
        metrics.queueDepth("reporting") { depth }
        val gauge = registry.get("kafka.publisher.queue.depth").tags("channel", "reporting").gauge()
        assertThat(gauge.value()).isEqualTo(3.0)
        depth = 7
        assertThat(gauge.value()).isEqualTo(7.0)
    }
}

class PipelineWithMockProducerTest {
    @Test
    fun `MockProducer-backed channel delivers through the whole pipeline`() {
        val producer = MockProducer(true, StringSerializer(), StringSerializer())
        val channels = ConfiguredEventPublisherChannels.create(
            properties = EventPublisherProperties(channels = mapOf("reporting" to ChannelSettings(enabled = true, lanes = 1))),
            sharedProducerConfig = mapOf("bootstrap.servers" to "unused:9092"),
            extraFailureHandlers = emptyList(),
            metrics = PublisherMetrics.None,
            applicationName = "pipeline-test",
            clock = MutableClock(),
            senderFactory = { KafkaRecordSender(producer) },
        )
        try {
            val result = channels.get("reporting").publishWithResult("t1", "42", SampleEvent("player_registered", "42"), mapOf("h" to "v")).awaitResult()
            assertThat(result.topic).isEqualTo("t1")
            assertThat(producer.history().single().value()).isEqualTo("""{"event_type":"player_registered","player_id":"42","seq":0}""")
        } finally {
            channels.close()
        }
        assertThat(producer.closed()).isTrue()
    }
}
