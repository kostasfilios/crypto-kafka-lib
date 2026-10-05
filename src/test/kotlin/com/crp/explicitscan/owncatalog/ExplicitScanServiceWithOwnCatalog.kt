package com.crp.explicitscan.owncatalog

import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.testing.RecordingEventPublisherChannels
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.FilterType

/** The explicit-scan shape with the service's own catalog bean: the auto-configuration must back off. */
@SpringBootApplication
@ComponentScan(
    basePackages = ["com.crp.system.libs.kafka"],
    excludeFilters = [ComponentScan.Filter(type = FilterType.REGEX, pattern = [".*Test.*", ".*\\.testsupport\\..*"])],
)
class ExplicitScanServiceWithOwnCatalog {
    @Bean
    fun serviceChannels(): EventPublisherChannels = RecordingEventPublisherChannels()
}
