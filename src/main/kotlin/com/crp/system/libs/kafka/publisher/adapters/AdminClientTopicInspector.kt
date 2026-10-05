package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.core.TopicInspector
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.DescribeTopicsOptions
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

/**
 * The startup topic check: describes the topics with a short-lived admin client built from the channel's
 * connection settings. Never creates a topic. Throws when the broker cannot answer within [missingTopics]'s timeout.
 */
internal class AdminClientTopicInspector(private val producerConfig: Map<String, Any>) : TopicInspector {
    override fun missingTopics(topics: Collection<String>, timeout: Duration): Set<String> {
        if (topics.isEmpty()) return emptySet()
        val deadlineNanos = System.nanoTime() + timeout.toNanos()
        val admin = AdminClient.create(adminConfig(producerConfig, timeout))
        try {
            val described = admin.describeTopics(topics, DescribeTopicsOptions().timeoutMs(timeout.toMillis().toInt())).topicNameValues()
            val missing = LinkedHashSet<String>()
            for ((topic, future) in described) {
                try {
                    future.get((deadlineNanos - System.nanoTime()).coerceAtLeast(0), TimeUnit.NANOSECONDS)
                } catch (e: ExecutionException) {
                    if (e.cause is UnknownTopicOrPartitionException) missing += topic else throw e
                }
            }
            return missing
        } finally {
            admin.close(Duration.ofSeconds(1))
        }
    }

    companion object {
        /** Only the keys an admin client knows (connection and security), so it logs no "unknown config" warnings. */
        fun adminConfig(producerConfig: Map<String, Any>, timeout: Duration): Map<String, Any> {
            val known = AdminClientConfig.configNames()
            val timeoutMs = timeout.toMillis().toInt()
            return HashMap<String, Any>().apply {
                producerConfig.forEach { (name, value) -> if (name in known) put(name, value) }
                put(AdminClientConfig.CLIENT_ID_CONFIG, "${producerConfig[ProducerConfig.CLIENT_ID_CONFIG] ?: "kafka-publisher"}-topic-check")
                put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs)
                put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs)
            }
        }
    }
}
