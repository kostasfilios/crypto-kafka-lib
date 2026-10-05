package com.crp.explicitscan

import com.crp.explicitscan.owncatalog.ExplicitScanServiceWithOwnCatalog
import com.crp.explicitscan.plain.ExplicitScanService
import com.crp.system.libs.kafka.publisher.adapters.MicrometerPublisherMetrics
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import com.crp.system.libs.kafka.publisher.testing.RecordingEventPublisherChannels
import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.kafka.core.KafkaTemplate

/** Services that declare an explicit `@ComponentScan` over the lib (review finding I1). */
class ExplicitComponentScanContextTest {

    private fun start(source: Class<*>): ConfigurableApplicationContext =
        SpringApplicationBuilder(source)
            .web(WebApplicationType.NONE)
            .properties(
                "kafka.bootstrap-servers=127.0.0.1:1",
                "spring.application.name=explicit-scan",
                "crypto.kafka.publisher.channels.reporting.enabled=true",
            )
            .run()

    @Test
    fun `an explicit component scan still gets one template, one catalog and the Micrometer adapter on the actuator registry`() {
        start(ExplicitScanService::class.java).use { context ->
            assertThat(context.getBeansOfType(KafkaTemplate::class.java)).hasSize(1)
            assertThat(context.getBeansOfType(EventPublisherChannels::class.java)).hasSize(1)
            assertThat(context.getBean(EventPublisherChannels::class.java).get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
            assertThat(context.getBean(PublisherMetrics::class.java)).isInstanceOf(MicrometerPublisherMetrics::class.java)
            val registry = context.getBean(MeterRegistry::class.java)
            assertThat(registry.find("kafka.publisher.queue.depth").tag("channel", "reporting").gauge()).isNotNull
        }
    }

    @Test
    fun `with an explicit component scan the auto-configuration still backs off for the service's own catalog`() {
        start(ExplicitScanServiceWithOwnCatalog::class.java).use { context ->
            assertThat(context.getBeansOfType(EventPublisherChannels::class.java)).containsOnlyKeys("serviceChannels")
            assertThat(context.getBean(EventPublisherChannels::class.java)).isInstanceOf(RecordingEventPublisherChannels::class.java)
        }
    }
}
