package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.EventPublisher
import com.crp.system.libs.kafka.publisher.api.PublishFailedException
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.api.PublishResult
import java.time.Clock
import java.util.concurrent.CompletableFuture

/** What `EventPublisherChannels.get` returns for a disabled or unknown channel: no threads, no producer, no logs. */
internal class DisabledEventPublisher(override val channel: String, private val clock: Clock) : EventPublisher {
    override fun publish(topic: String, key: String?, event: Any, headers: Map<String, String>) = Unit

    override fun publishJson(topic: String, key: String?, json: String, headers: Map<String, String>) = Unit

    override fun publishWithResult(topic: String, key: String?, event: Any, headers: Map<String, String>): CompletableFuture<PublishResult> =
        CompletableFuture.failedFuture(
            PublishFailedException(PublishFailure(channel, topic, key, CHANNEL_UNAVAILABLE, 0, false, null, null, clock.instant())),
        )
}
