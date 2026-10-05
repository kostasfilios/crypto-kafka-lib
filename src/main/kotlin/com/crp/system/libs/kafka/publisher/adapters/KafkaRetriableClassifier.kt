package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.PublishFailureClassifier
import org.apache.kafka.common.errors.RetriableException

/** Retriable = a Kafka `RetriableException` anywhere in the first 10 links of the cause chain. */
internal object KafkaRetriableClassifier : PublishFailureClassifier {
    override fun isRetriable(error: Throwable): Boolean =
        generateSequence(error) { it.cause }.take(10).any { it is RetriableException }
}
