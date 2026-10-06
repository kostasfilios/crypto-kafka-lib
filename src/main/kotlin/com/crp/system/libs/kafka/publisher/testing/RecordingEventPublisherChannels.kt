package com.crp.system.libs.kafka.publisher.testing

import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.spring.SerializerNaming
import java.util.concurrent.ConcurrentHashMap

/**
 * [EventPublisherChannels] for service tests: every name is a [RecordingEventPublisher], created on first use.
 *
 * ```
 * val channels = RecordingEventPublisherChannels()
 * SelfExclusionReportingEvents(channels, "account.responsible_gambling.events").lifted(event)
 * assertThat(channels.recorded("reporting").single().key).isEqualTo("42")
 * ```
 */
class RecordingEventPublisherChannels @JvmOverloads constructor(
    /** Serializer naming per channel; SNAKE_CASE when absent. */
    private val naming: Map<String, SerializerNaming> = emptyMap(),
) : EventPublisherChannels {
    private val publishers = ConcurrentHashMap<String, RecordingEventPublisher>()

    override fun get(name: String): RecordingEventPublisher =
        publishers.computeIfAbsent(name) { RecordingEventPublisher(it, naming[it] ?: SerializerNaming.SNAKE_CASE) }

    override fun names(): Set<String> = publishers.keys.toSet()

    /** What channel [name] accepted, in publish order; empty when nothing used it. */
    fun recorded(name: String): List<RecordedEvent> = publishers[name]?.recorded ?: emptyList()

    fun clear() = publishers.values.forEach { it.clear() }
}
