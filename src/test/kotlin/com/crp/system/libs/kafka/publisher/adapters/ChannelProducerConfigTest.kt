package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.spring.ProducerSettings
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test

class ChannelProducerConfigTest {
    private val shared: Map<String, Any> = mapOf(
        "bootstrap.servers" to "kafka-1:9092,kafka-2:9092",
        "security.protocol" to "SASL_SSL",
        "sasl.mechanism" to "SCRAM-SHA-512",
        "sasl.jaas.config" to "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";",
        "ssl.truststore.location" to "/etc/kafka/truststore.jks",
        "client.id" to "AccountServices",
        "transactional.id" to "tx-1",
        "key.serializer" to ByteArraySerializer::class.java,
        "value.serializer" to ByteArraySerializer::class.java,
        "max.block.ms" to 60_000,
        "buffer.memory" to 33_554_432L,
    )

    private fun build(settings: ProducerSettings = ProducerSettings()) = ChannelProducerConfig.build(shared, "reporting", settings, "KYCService")

    @Test
    fun `keeps the service's connection and security settings`() {
        assertThat(build()).contains(
            entry("bootstrap.servers", "kafka-1:9092,kafka-2:9092"),
            entry("security.protocol", "SASL_SSL"),
            entry("sasl.mechanism", "SCRAM-SHA-512"),
            entry("sasl.jaas.config", shared["sasl.jaas.config"]),
            entry("ssl.truststore.location", "/etc/kafka/truststore.jks"),
        )
    }

    @Test
    fun `applies the channel's limits over the service's`() {
        assertThat(build()).contains(
            entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000L),
            entry(ProducerConfig.BUFFER_MEMORY_CONFIG, 8L * 1024 * 1024),
            entry(ProducerConfig.LINGER_MS_CONFIG, 5),
            entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000),
            entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000),
            entry(ProducerConfig.ACKS_CONFIG, "all"),
            entry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"),
        )
        val custom = build(ProducerSettings(maxBlockMs = 500, bufferMemory = 1_048_576, lingerMs = 20, requestTimeoutMs = 3_000, deliveryTimeoutMs = 9_000, compressionType = "lz4"))
        assertThat(custom).contains(
            entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500L),
            entry(ProducerConfig.BUFFER_MEMORY_CONFIG, 1_048_576L),
            entry(ProducerConfig.LINGER_MS_CONFIG, 20),
            entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000),
            entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 9_000),
            entry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"),
        )
    }

    @Test
    fun `names the client after the application and the channel`() {
        assertThat(build()[ProducerConfig.CLIENT_ID_CONFIG]).isEqualTo("KYCService-reporting")
    }

    @Test
    fun `is never transactional`() {
        assertThat(build()).doesNotContainKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
    }

    @Test
    fun `always uses String serializers`() {
        assertThat(build()).contains(
            entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java),
            entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java),
        )
    }

    @Test
    fun `turns idempotence on only with acks=all`() {
        assertThat(build(ProducerSettings(acks = "all"))[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG]).isEqualTo(true)
        assertThat(build(ProducerSettings(acks = "-1"))[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG]).isEqualTo(true)
        assertThat(build(ProducerSettings(acks = "1"))[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG]).isEqualTo(false)
        assertThat(build(ProducerSettings(acks = "0"))[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG]).isEqualTo(false)
        assertThat(build(ProducerSettings(acks = "1"))[ProducerConfig.ACKS_CONFIG]).isEqualTo("1")
    }

    @Test
    fun `applies extra keys last, so they can override anything`() {
        val config = build(ProducerSettings(extra = mapOf("linger.ms" to "50", "max.in.flight.requests.per.connection" to "1", "client.id" to "custom")))
        assertThat(config).contains(
            entry("linger.ms", "50"),
            entry("max.in.flight.requests.per.connection", "1"),
            entry("client.id", "custom"),
        )
    }

    @Test
    fun `does not change the service's map`() {
        val before = HashMap(shared)
        build(ProducerSettings(extra = mapOf("linger.ms" to "50")))
        assertThat(shared).isEqualTo(before)
    }

    @Test
    fun `the result passes Kafka's own producer validation for every acks value`() {
        listOf("all", "-1", "1", "0").forEach { acks ->
            assertDoesNotThrow({ ProducerConfig(build(ProducerSettings(acks = acks))) }, acks)
        }
    }
}
