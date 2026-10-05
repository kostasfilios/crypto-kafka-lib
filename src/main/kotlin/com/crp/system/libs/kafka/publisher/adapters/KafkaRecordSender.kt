package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.core.OutboundRecord
import com.crp.system.libs.kafka.publisher.core.RecordSender
import com.crp.system.libs.kafka.publisher.core.SendOutcome
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import java.time.Duration

/**
 * Sends through the channel's own producer. kafka-clients 3.x reports most send errors through the callback
 * (topic missing from metadata after `max.block.ms`, buffer full, record too large): those are DELIVERY_FAILED.
 * `send` itself throws only for a closed producer, an interrupt or a non-API KafkaException (SEND_REJECTED).
 */
internal class KafkaRecordSender(private val producer: Producer<String, String>) : RecordSender {
    override fun send(record: OutboundRecord, onOutcome: (SendOutcome) -> Unit) {
        val headers: List<Header> = record.headers.map { (name, value) -> RecordHeader(name, value.toByteArray(Charsets.UTF_8)) }
        producer.send(ProducerRecord(record.topic, null, record.key, record.payload, headers)) { metadata, error ->
            onOutcome(if (error != null) SendOutcome.Failed(error) else SendOutcome.Delivered(metadata.partition(), metadata.offset()))
        }
    }

    override fun close(timeout: Duration) = producer.close(timeout)
}
