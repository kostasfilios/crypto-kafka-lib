package com.crp.system.libs.kafka.publisher.testing

import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.spring.SerializerNaming
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test

class RecordingEventPublisherTest {
    data class SelfExclusionReportingEvent(val eventType: String, val playerId: Long, val reason: String?)

    /** The proposal's "after" example. */
    class SelfExclusionReportingEvents(channels: EventPublisherChannels, private val topic: String) {
        private val reporting = channels.get("reporting")
        fun lifted(event: SelfExclusionReportingEvent) = reporting.publish(topic, event.playerId.toString(), event)
    }

    class Node(val name: String) {
        var next: Node? = null
    }

    private val event = SelfExclusionReportingEvent("self_exclusion_lifted", 42, null)

    @Test
    fun `the proposal's service test works without a broker`() {
        val channels = RecordingEventPublisherChannels()
        SelfExclusionReportingEvents(channels, "account.responsible_gambling.events").lifted(event)

        assertThat(channels.recorded("reporting").single().key).isEqualTo("42")
        assertThat(channels.recorded("reporting").single()).isEqualTo(
            RecordedEvent(
                channel = "reporting",
                topic = "account.responsible_gambling.events",
                key = "42",
                event = event,
                json = """{"event_type":"self_exclusion_lifted","player_id":42,"reason":null}""",
                headers = emptyMap(),
            ),
        )
    }

    @Test
    fun `the recorded JSON is what the real channel's serializer produces`() {
        val channels = RecordingEventPublisherChannels()
        val reporting = channels.get("reporting")
        reporting.publish("t1", "42", event)
        assertThat(reporting.recorded.single().json).isEqualTo(reporting.serializer.serialize(event))
    }

    @Test
    fun `naming can be set per channel`() {
        val channels = RecordingEventPublisherChannels(mapOf("balance-updates" to SerializerNaming.IDENTITY))
        channels.get("balance-updates").publish("t1", "42", event)
        channels.get("reporting").publish("t1", "42", event)
        assertThat(channels.recorded("balance-updates").single().json).isEqualTo("""{"eventType":"self_exclusion_lifted","playerId":42,"reason":null}""")
        assertThat(channels.recorded("reporting").single().json).startsWith("""{"event_type"""")
    }

    @Test
    fun `publishJson is recorded unchanged, with its headers as they were at the call`() {
        val channels = RecordingEventPublisherChannels()
        val headers = mutableMapOf("source_service" to "KYCService")
        channels.get("audit").publishJson("user-kyc-status-updates", "42", """{"userId":42}""", headers)
        headers["source_service"] = "changed"
        assertThat(channels.recorded("audit").single()).isEqualTo(
            RecordedEvent("audit", "user-kyc-status-updates", "42", null, """{"userId":42}""", mapOf("source_service" to "KYCService")),
        )
    }

    @Test
    fun `publishWithResult completes with increasing offsets`() {
        val reporting = RecordingEventPublisherChannels().get("reporting")
        val first = reporting.publishWithResult("t1", "1", event).awaitResult()
        val second = reporting.publishWithResult("t1", "2", event).awaitResult()
        assertThat(first.offset).isEqualTo(0)
        assertThat(second.offset).isEqualTo(1)
        assertThat(second.attempts).isEqualTo(1)
        assertThat(second.key).isEqualTo("2")
    }

    @Test
    fun `an event that cannot be serialized never throws and is a SERIALIZATION failure, like the real channel`() {
        val reporting = RecordingEventPublisherChannels().get("reporting")
        // a two-node cycle (Gson skips a field that points at its own object, but not a longer loop): StackOverflowError
        val cyclic = Node("a").also { a -> a.next = Node("b").also { b -> b.next = a } }

        assertDoesNotThrow { reporting.publish("t1", "1", cyclic) }
        val failure = reporting.publishWithResult("t1", "2", cyclic).awaitFailure()

        assertThat(failure.stage).isEqualTo(PublishFailureStage.SERIALIZATION)
        assertThat(reporting.recorded).isEmpty()
        assertThat(reporting.failures).hasSize(2)
    }

    @Test
    fun `names, clear, and an unused channel`() {
        val channels = RecordingEventPublisherChannels()
        channels.get("reporting").publish("t1", "1", event)
        channels.get("audit")
        assertThat(channels.names()).containsExactlyInAnyOrder("reporting", "audit")
        assertThat(channels.recorded("never-used")).isEmpty()
        channels.clear()
        assertThat(channels.recorded("reporting")).isEmpty()
        assertThat(channels.get("reporting")).isSameAs(channels.get("reporting"))
    }
}
