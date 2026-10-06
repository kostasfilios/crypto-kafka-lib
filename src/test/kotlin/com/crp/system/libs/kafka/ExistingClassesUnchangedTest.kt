package com.crp.system.libs.kafka

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit

/** v1.3.0 leaves the v1.2.0 classes alone: these pin their behaviour (v1.2.0 shipped no tests of its own). */
class ExistingClassesUnchangedTest {

    @Test
    fun `KafkaProducerConfig still sets only the bootstrap servers and String serializers`() {
        val config = KafkaProducerConfig("kafka:9092").producerFactory().configurationProperties
        assertThat(config).isEqualTo(
            mapOf(
                "bootstrap.servers" to "kafka:9092",
                "key.serializer" to StringSerializer::class.java,
                "value.serializer" to StringSerializer::class.java,
            ),
        )
    }

    @Test
    fun `KafkaMessageProducer still sends keyless records, fire-and-forget or with a result`() {
        val mock = MockProducer(true, StringSerializer(), StringSerializer())
        val shared = object : Producer<String, String> by mock {
            override fun close() = Unit
            override fun close(timeout: Duration) = Unit
        }
        val producer = KafkaMessageProducer(KafkaTemplate(ProducerFactory<String, String> { shared }))

        producer.sendMessage("t1", "m1")
        val result = producer.sendMessageWithResult("t1", "m2").get(5, TimeUnit.SECONDS)

        assertThat(mock.history().map { it.key() to it.value() }).containsExactly(null to "m1", null to "m2")
        assertThat(result!!.recordMetadata.topic()).isEqualTo("t1")
    }

    @Test
    fun `KafkaConsumerConfig still builds a String consumer factory for the group`() {
        val factory = KafkaConsumerConfig("kafka:9092", "group-1").consumerFactory()
        assertThat(factory.configurationProperties).containsEntry("bootstrap.servers", "kafka:9092").containsEntry("group.id", "group-1")
    }
}
