package com.crp.system.libs.kafka.publisher.core

import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.testsupport.MutableClock
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test

class DisabledEventPublisherTest {
    private val clock = MutableClock()
    private val publisher = DisabledEventPublisher("reporting", clock)

    @Test
    fun `publish and publishJson do nothing and never throw`() {
        assertDoesNotThrow { publisher.publish("t1", "k", SampleEvent("x", "1")) }
        assertDoesNotThrow { publisher.publishJson("t1", "k", "{}") }
        assertThat(publisher.channel).isEqualTo("reporting")
    }

    @Test
    fun `publishWithResult fails at once with CHANNEL_UNAVAILABLE`() {
        val failure = publisher.publishWithResult("t1", "k", SampleEvent("x", "1")).awaitFailure()
        assertThat(failure).isEqualTo(PublishFailure("reporting", "t1", "k", CHANNEL_UNAVAILABLE, 0, false, null, null, clock.instant()))
    }
}
