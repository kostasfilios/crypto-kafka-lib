package com.crp.system.libs.kafka.publisher.spring

import com.crp.system.libs.kafka.publisher.adapters.MicrometerPublisherMetrics
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.context.TypeExcludeFilter
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.FilterType
import org.springframework.kafka.core.KafkaTemplate

/**
 * A service as the PAM services are built: `@SpringBootApplication`'s scan covers `com.crp.system`, so it also
 * scans the lib's packages, with the actuator's MeterRegistry on the classpath. (The scan below is
 * `@SpringBootApplication`'s own, plus a filter that keeps this test module's classes out.)
 */
class ServiceScanContextTest {

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @ComponentScan(
        basePackages = ["com.crp.system.libs.kafka"],
        excludeFilters = [
            ComponentScan.Filter(type = FilterType.CUSTOM, classes = [TypeExcludeFilter::class]),
            ComponentScan.Filter(type = FilterType.CUSTOM, classes = [AutoConfigurationExcludeFilter::class]),
            ComponentScan.Filter(type = FilterType.REGEX, pattern = [".*Test.*", ".*\\.testsupport\\..*"]),
        ],
    )
    class ServiceLikeApplication

    @Test
    fun `a service whose scan covers the lib gets one template, one catalog and the Micrometer adapter on the actuator registry`() {
        SpringApplicationBuilder(ServiceLikeApplication::class.java)
            .web(WebApplicationType.NONE)
            .properties(
                "kafka.bootstrap-servers=127.0.0.1:1",
                "spring.application.name=scan-test",
                "crypto.kafka.publisher.channels.reporting.enabled=true",
            )
            .run().use { context ->
                assertThat(context.getBeansOfType(KafkaTemplate::class.java)).hasSize(1)
                assertThat(context.getBeansOfType(EventPublisherChannels::class.java)).hasSize(1)
                assertThat(context.getBean(EventPublisherChannels::class.java).get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
                assertThat(context.getBean(PublisherMetrics::class.java)).isInstanceOf(MicrometerPublisherMetrics::class.java)
                val registry = context.getBean(MeterRegistry::class.java)
                assertThat(registry.find("kafka.publisher.queue.depth").tag("channel", "reporting").gauge()).isNotNull
            }
    }
}
