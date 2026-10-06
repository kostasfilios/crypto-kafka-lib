package com.crp.system.libs.kafka.publisher.nomicrometer

import com.crp.system.libs.kafka.KafkaProducerConfig
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import com.crp.system.libs.kafka.publisher.spring.EventPublisherAutoConfiguration
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner

/**
 * Runs in the `testWithoutMicrometer` Gradle task, whose classpath has no micrometer-core and no actuator:
 * a service without Micrometer. (Excluded from the normal `test` task.)
 */
class NoMicrometerContextTest {

    @Test
    fun `micrometer-core is really absent from this classpath`() {
        assertThrows<ClassNotFoundException> { Class.forName("io.micrometer.core.instrument.MeterRegistry") }
    }

    @Test
    fun `the lib loads, the channel works, and the metrics fall back to None`() {
        ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(EventPublisherAutoConfiguration::class.java))
            .withUserConfiguration(KafkaProducerConfig::class.java)
            .withPropertyValues(
                "kafka.bootstrap-servers=127.0.0.1:1",
                "crypto.kafka.publisher.channels.reporting.enabled=true",
                "crypto.kafka.publisher.channels.reporting.producer.max-block-ms=200",
            )
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context.getBeansOfType(PublisherMetrics::class.java)).isEmpty()
                val reporting = context.getBean(EventPublisherChannels::class.java).get("reporting")
                assertThat(reporting).isInstanceOf(ChannelEventPublisher::class.java)
                // nothing listens on the bootstrap port: the record went all the way to the producer and failed there
                assertThat(reporting.publishWithResult("t1", "42", SampleEvent("x", "42")).awaitFailure().stage)
                    .isEqualTo(PublishFailureStage.DELIVERY_FAILED)
            }
    }
}
