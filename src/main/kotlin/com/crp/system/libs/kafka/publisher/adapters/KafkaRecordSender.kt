package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.core.OutboundRecord
import com.crp.system.libs.kafka.publisher.core.RecordSender
import com.crp.system.libs.kafka.publisher.core.SendOutcome
import org.apache.kafka.clients.producer.BufferExhaustedException
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TimeoutException
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
        val sendingThread = Thread.currentThread()
        var insideSend = true
        try {
            producer.send(ProducerRecord(record.topic, null, record.key, record.payload, headers)) { metadata, error ->
                onOutcome(
                    if (error == null) SendOutcome.Delivered(metadata.partition(), metadata.offset())
                    else SendOutcome.Failed(error, topicMissing = insideSend && Thread.currentThread() === sendingThread && isMissingFromMetadata(error)),
                )
            }
        } finally {
            insideSend = false
        }
    }

    override fun close(timeout: Duration) = producer.close(timeout)

    companion object {
        /**
         * The producer reports a topic it cannot find in the broker's metadata within `max.block.ms` as a
         * TimeoutException passed to the callback inside `send()`. A full buffer is reported the same way but as a
         * BufferExhaustedException (a TimeoutException subclass), which is not a missing topic. No message matching;
         * callers also require the callback to have run synchronously inside `send()`.
         */
        fun isMissingFromMetadata(error: Exception): Boolean = error is TimeoutException && error !is BufferExhaustedException
    }
}
