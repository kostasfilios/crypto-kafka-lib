package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.spring.ProducerSettings
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

/** The channel's producer config: the service's connection settings (from the one KafkaTemplate) + the channel's limits. */
internal object ChannelProducerConfig {
    fun build(shared: Map<String, Any>, channel: String, settings: ProducerSettings, applicationName: String): Map<String, Any> =
        HashMap(shared).apply {
            remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG) // never transactional (AD9)
            put(ProducerConfig.CLIENT_ID_CONFIG, "$applicationName-$channel")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, settings.acks)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, settings.acks == "all" || settings.acks == "-1")
            put(ProducerConfig.MAX_BLOCK_MS_CONFIG, settings.maxBlockMs)
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.bufferMemory)
            put(ProducerConfig.LINGER_MS_CONFIG, settings.lingerMs)
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, settings.requestTimeoutMs)
            put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, settings.deliveryTimeoutMs)
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, settings.compressionType)
            putAll(settings.extra)
        }
}
