package com.crp.system.libs.kafka.publisher.spring

import com.crp.system.libs.kafka.publisher.adapters.MicrometerPublisherMetrics
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.BindException
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertyName
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import org.springframework.kafka.core.KafkaTemplate
import java.time.Clock
import java.time.Duration

/**
 * Registers one bean, [EventPublisherChannels], and no Kafka beans (AD9): each enabled channel builds its own
 * producer from the service's one `KafkaTemplate`. Inert until a channel is configured.
 */
@AutoConfiguration(afterName = ["org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration"])
@ConditionalOnClass(KafkaTemplate::class)
@EnableConfigurationProperties(EventPublisherProperties::class)
class EventPublisherAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(EventPublisherChannels::class)
    fun eventPublisherChannels(
        properties: EventPublisherProperties,
        kafkaTemplate: ObjectProvider<KafkaTemplate<String, String>>,
        extraFailureHandlers: ObjectProvider<PublishFailureHandler>,
        metrics: ObjectProvider<PublisherMetrics>,
        environment: Environment,
    ): EventPublisherChannels = ConfiguredEventPublisherChannels.create(
        properties = properties,
        sharedProducerConfig = sharedProducerConfig(kafkaTemplate), // null -> channels off + ERROR
        extraFailureHandlers = extraFailureHandlers.orderedStream().toList(),
        metrics = metrics.ifAvailable ?: PublisherMetrics.None,
        applicationName = environment.getProperty("spring.application.name", "pam"),
        clock = Clock.systemUTC(),
        bindingErrors = bindingErrors(environment, properties.channels.keys),
    )

    /**
     * Micrometer, only when the service has it and a `MeterRegistry` bean; a service's own `PublisherMetrics` bean wins.
     *
     * Deliberately not annotated `@Configuration`: the services' `@SpringBootApplication` scan covers
     * `com.crp.system.libs.kafka`, and a nested `@Configuration` class would be picked up by that scan as a regular
     * configuration, so its `@ConditionalOnBean(MeterRegistry)` would run before the actuator's registry exists and
     * the metrics would silently stay off. Without the stereotype it is processed only as a member of this
     * auto-configuration, after `CompositeMeterRegistryAutoConfiguration`.
     */
    @ConditionalOnClass(name = ["io.micrometer.core.instrument.MeterRegistry"])
    class MicrometerMetricsConfiguration {
        @Bean
        @ConditionalOnBean(type = ["io.micrometer.core.instrument.MeterRegistry"])
        @ConditionalOnMissingBean(PublisherMetrics::class)
        fun micrometerPublisherMetrics(registry: MeterRegistry): PublisherMetrics = MicrometerPublisherMetrics(registry)
    }

    private fun sharedProducerConfig(kafkaTemplate: ObjectProvider<KafkaTemplate<String, String>>): Map<String, Any>? =
        try {
            kafkaTemplate.ifUnique?.producerFactory?.configurationProperties
        } catch (e: UnsupportedOperationException) {
            // a custom ProducerFactory that cannot share its settings: the channels stay off, the service starts (AD10)
            logger.error("kafka_publisher_shared_config_unavailable cause={}", e.toString())
            null
        }

    /**
     * The properties bind leniently (`ignoreInvalidFields`); this strict re-bind finds the channels whose values do not
     * convert, so they are switched off with an ERROR instead of running on defaults.
     */
    private fun bindingErrors(environment: Environment, channels: Collection<String>): Map<String, String> {
        val binder = Binder.get(environment)
        try {
            binder.bind("${EventPublisherProperties.PREFIX}.summary-interval", Duration::class.java)
        } catch (e: BindException) {
            logger.error("kafka_publisher_invalid_setting summary-interval reason={} using={}", reason(e), ConfiguredEventPublisherChannels.DEFAULT_SUMMARY_INTERVAL)
        }
        val errors = LinkedHashMap<String, String>()
        for (channel in channels) {
            try {
                val dotted = "${EventPublisherProperties.PREFIX}.channels.$channel"
                val name = if (ConfigurationPropertyName.isValid(dotted)) ConfigurationPropertyName.of(dotted)
                else ConfigurationPropertyName.of("${EventPublisherProperties.PREFIX}.channels").append("[$channel]")
                binder.bind(name, Bindable.of(ChannelSettings::class.java))
            } catch (e: BindException) {
                errors[channel] = reason(e)
            } catch (e: Exception) {
                logger.warn("kafka_publisher_binding_check_skipped channel={} cause={}", channel, e.toString())
            }
        }
        return errors
    }

    private fun reason(e: BindException): String {
        val root = generateSequence(e as Throwable) { it.cause }.take(20).last()
        return "unconvertible_value property=${e.name} cause=${root.javaClass.simpleName}: ${root.message}"
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(EventPublisherAutoConfiguration::class.java)
    }
}
