package com.crp.system.libs.kafka.publisher.spring

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.KafkaProducerConfig
import com.crp.system.libs.kafka.publisher.adapters.MicrometerPublisherMetrics
import com.crp.system.libs.kafka.publisher.api.EventPublisherChannels
import com.crp.system.libs.kafka.publisher.api.PublishFailure
import com.crp.system.libs.kafka.publisher.api.PublishFailureHandler
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.core.ChannelEventPublisher
import com.crp.system.libs.kafka.publisher.core.DisabledEventPublisher
import com.crp.system.libs.kafka.publisher.testing.RecordingEventPublisherChannels
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.RecordingFailureHandler
import com.crp.system.libs.kafka.publisher.testsupport.RecordingMetrics
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.BeanCurrentlyInCreationException
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.annotation.ImportCandidates
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Supplier

class EventPublisherAutoConfigurationTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventPublisherAutoConfiguration::class.java))
        .withPropertyValues("kafka.bootstrap-servers=127.0.0.1:1", "spring.application.name=ctx-test")

    /** A service as they are today: the lib's KafkaProducerConfig gives the one KafkaTemplate. */
    private val service = runner.withUserConfiguration(KafkaProducerConfig::class.java)

    private val reportingOn = "crypto.kafka.publisher.channels.reporting.enabled=true"

    private fun channels(context: org.springframework.context.ApplicationContext) = context.getBean(EventPublisherChannels::class.java)

    @Test
    fun `the auto-configuration is registered in AutoConfiguration imports`() {
        val candidates = ImportCandidates.load(AutoConfiguration::class.java, javaClass.classLoader).candidates
        assertThat(candidates).contains(EventPublisherAutoConfiguration::class.java.name)
    }

    @Test
    fun `with the lib's KafkaProducerConfig there is exactly one KafkaTemplate, and the publisher adds no Kafka beans`() {
        service.withPropertyValues(reportingOn).run { context ->
            assertThat(context).hasNotFailed()
            assertThat(context.getBeansOfType(KafkaTemplate::class.java)).hasSize(1).containsKey("kafkaTemplate")
            assertThat(context.getBeansOfType(ProducerFactory::class.java)).hasSize(1).containsKey("producerFactory")
            assertThat(context).hasSingleBean(EventPublisherChannels::class.java)
            assertThat(channels(context).get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
        }
    }

    @Test
    fun `without channel properties the auto-configuration is inert`() {
        service.run { context ->
            assertThat(context).hasNotFailed()
            assertThat(channels(context).names()).isEmpty()
            assertThat(context.getBean(EventPublisherProperties::class.java)).isEqualTo(EventPublisherProperties())
        }
    }

    @Test
    fun `channels bind from properties, and Kotlin defaults fill every missing key, nested ones included`() {
        runner.withPropertyValues(
            "crypto.kafka.publisher.summary-interval=3s",
            "crypto.kafka.publisher.channels.reporting.enabled=true",
            "crypto.kafka.publisher.channels.reporting.lanes=4",
            "crypto.kafka.publisher.channels.reporting.backpressure=block-with-timeout",
            "crypto.kafka.publisher.channels.reporting.block-timeout=250ms",
            "crypto.kafka.publisher.channels.reporting.retry.max-attempts=3",
            "crypto.kafka.publisher.channels.reporting.producer.max-block-ms=1500",
            "crypto.kafka.publisher.channels.reporting.producer.extra.max.in.flight.requests.per.connection=1",
            "crypto.kafka.publisher.channels.reporting.topics=account.player.events,payments.request.events",
            "crypto.kafka.publisher.channels.balance-updates.serializer=IDENTITY",
        ).run { context ->
            val properties = context.getBean(EventPublisherProperties::class.java)
            assertThat(properties.summaryInterval).isEqualTo(Duration.ofSeconds(3))
            assertThat(properties.channels.getValue("reporting")).isEqualTo(
                ChannelSettings(
                    enabled = true,
                    lanes = 4,
                    backpressure = BackpressureMode.BLOCK_WITH_TIMEOUT,
                    blockTimeout = Duration.ofMillis(250),
                    retry = RetrySettings(maxAttempts = 3),
                    producer = ProducerSettings(maxBlockMs = 1_500, extra = mapOf("max.in.flight.requests.per.connection" to "1")),
                    topics = listOf("account.player.events", "payments.request.events"),
                ),
            )
            assertThat(properties.channels.getValue("balance-updates")).isEqualTo(ChannelSettings(serializer = SerializerNaming.IDENTITY))
        }
    }

    @Test
    fun `the proposal's reporting block binds to exactly the proposed values`() {
        runner.withPropertyValues(
            "crypto.kafka.publisher.summary-interval=10s",
            "crypto.kafka.publisher.channels.reporting.enabled=\${reporting.events.enabled:false}",
            "crypto.kafka.publisher.channels.reporting.serializer=SNAKE_CASE",
            "crypto.kafka.publisher.channels.reporting.lanes=2",
            "crypto.kafka.publisher.channels.reporting.queue-capacity=10000",
            "crypto.kafka.publisher.channels.reporting.backpressure=DROP",
            "crypto.kafka.publisher.channels.reporting.ordering=PER_KEY",
            "crypto.kafka.publisher.channels.reporting.retry.max-attempts=1",
            "crypto.kafka.publisher.channels.reporting.producer.max-block-ms=2000",
            "crypto.kafka.publisher.channels.reporting.producer.buffer-memory=8388608",
            "crypto.kafka.publisher.channels.reporting.producer.linger-ms=5",
            "crypto.kafka.publisher.channels.reporting.producer.delivery-timeout-ms=30000",
            "crypto.kafka.publisher.channels.reporting.topics=account.player.events,payments.request.events",
            "crypto.kafka.publisher.channels.reporting.shutdown-timeout=5s",
        ).run { context ->
            val properties = context.getBean(EventPublisherProperties::class.java)
            assertThat(properties).isEqualTo(
                EventPublisherProperties(
                    channels = mapOf("reporting" to ChannelSettings(topics = listOf("account.player.events", "payments.request.events"))),
                ),
            )
        }
    }

    @Test
    fun `the reporting flag placeholder switches the channel off by default and on with the flag`() {
        val reporting = "crypto.kafka.publisher.channels.reporting.enabled=\${reporting.events.enabled:false}"
        service.withPropertyValues(reporting).run { context ->
            assertThat(context.getBean(EventPublisherProperties::class.java).channels.getValue("reporting").enabled).isFalse()
            assertThat(channels(context).get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
        }
        service.withPropertyValues(reporting, "reporting.events.enabled=true").run { context ->
            assertThat(context.getBean(EventPublisherProperties::class.java).channels.getValue("reporting").enabled).isTrue()
            assertThat(channels(context).get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
        }
    }

    @Test
    fun `the Micrometer adapter is used only when a MeterRegistry bean exists`() {
        service.withPropertyValues(reportingOn).withBean(MeterRegistry::class.java, Supplier<MeterRegistry> { SimpleMeterRegistry() }).run { context ->
            assertThat(context.getBean(PublisherMetrics::class.java)).isInstanceOf(MicrometerPublisherMetrics::class.java)
            val registry = context.getBean(MeterRegistry::class.java)
            assertThat(registry.find("kafka.publisher.queue.depth").tag("channel", "reporting").gauge()).isNotNull
        }
        service.withPropertyValues(reportingOn).run { context ->
            assertThat(context).doesNotHaveBean(PublisherMetrics::class.java)
            assertThat(channels(context).get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
        }
    }

    @Test
    fun `a service's own PublisherMetrics bean wins over the Micrometer adapter`() {
        service.withPropertyValues(reportingOn)
            .withBean(MeterRegistry::class.java, Supplier<MeterRegistry> { SimpleMeterRegistry() })
            .withBean(RecordingMetrics::class.java)
            .run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context.getBeansOfType(PublisherMetrics::class.java).values.single()).isInstanceOf(RecordingMetrics::class.java)
                assertThat(context.getBean(RecordingMetrics::class.java).gauges).containsKey("reporting")
            }
    }

    @Test
    fun `no unique KafkaTemplate switches the channels off with an ERROR, and the context still starts`() {
        LogCapture(ConfiguredEventPublisherChannels::class.java).use { logs ->
            runner.withPropertyValues(reportingOn).run { context -> // no template at all
                assertThat(context).hasNotFailed()
                assertThat(channels(context).get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
            }
            runner.withUserConfiguration(TwoTemplates::class.java).withPropertyValues(reportingOn).run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context.getBeansOfType(KafkaTemplate::class.java)).hasSize(2)
                assertThat(channels(context).get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
            }
            assertThat(logs.lines(Level.ERROR)).containsOnly("kafka_publisher_channel_disabled channel=reporting reason=no_unique_kafka_template").hasSize(2)
        }
    }

    @Test
    fun `with a second @Primary KafkaTemplate, the KYCService shape, the channels build from the primary one`() {
        LogCapture(ConfiguredEventPublisherChannels::class.java).use { logs ->
            service.withUserConfiguration(PrimaryTemplate::class.java).withPropertyValues(reportingOn).run { context ->
                assertThat(context).hasNotFailed()
                assertThat(context.getBeansOfType(KafkaTemplate::class.java)).hasSize(2)
                assertThat(channels(context).get("reporting")).isInstanceOf(ChannelEventPublisher::class.java)
            }
            assertThat(logs.lines(Level.INFO)).anyMatch {
                it.startsWith("kafka_publisher_channel_enabled channel=reporting client_id=ctx-test-reporting bootstrap=127.0.0.1:2 ")
            }
            assertThat(logs.lines(Level.ERROR)).isEmpty()
        }
    }

    @Test
    fun `a ProducerFactory that cannot share its settings switches the channels off, and the context still starts`() {
        runner.withUserConfiguration(OpaqueProducerFactory::class.java).withPropertyValues(reportingOn).run { context ->
            assertThat(context).hasNotFailed()
            assertThat(channels(context).get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
        }
    }

    @Test
    fun `invalid channel settings never stop the service`() {
        service.withPropertyValues(reportingOn, "crypto.kafka.publisher.channels.reporting.lanes=0").run { context ->
            assertThat(context).hasNotFailed()
            assertThat(channels(context).get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
        }
    }

    @Test
    fun `a value of the wrong type never stops the service, that channel is switched off with an ERROR`() {
        LogCapture(ConfiguredEventPublisherChannels::class.java).use { logs ->
            service.withPropertyValues(
                reportingOn,
                "crypto.kafka.publisher.channels.reporting.backpressure=CALLER_RUN",
                "crypto.kafka.publisher.channels.audit.enabled=true",
            ).run { context ->
                assertThat(context).hasNotFailed()
                assertThat(channels(context).get("reporting")).isInstanceOf(DisabledEventPublisher::class.java)
                assertThat(channels(context).get("audit")).isInstanceOf(ChannelEventPublisher::class.java)
            }
            assertThat(logs.lines(Level.ERROR)).anyMatch {
                it.startsWith("kafka_publisher_channel_invalid channel=reporting reason=") && it.contains("crypto.kafka.publisher.channels.reporting.backpressure")
            }
        }
    }

    @Test
    fun `a PublishFailureHandler bean is called for every channel`() {
        service.withPropertyValues(reportingOn, "crypto.kafka.publisher.channels.reporting.producer.max-block-ms=200")
            .withBean(RecordingFailureHandler::class.java)
            .run { context ->
                channels(context).get("reporting").publish("t1", "k", SampleEvent("x", "1"))
                val failure = context.getBean(RecordingFailureHandler::class.java).single()
                assertThat(failure.stage).isEqualTo(PublishFailureStage.DELIVERY_FAILED) // nothing listens on the bootstrap port
                assertThat(failure.channel).isEqualTo("reporting")
            }
    }

    @Test
    fun `a service can replace the catalog, for example with the recording one in its tests`() {
        service.withPropertyValues(reportingOn).withBean(EventPublisherChannels::class.java, Supplier<EventPublisherChannels> { RecordingEventPublisherChannels() })
            .run { context ->
                assertThat(context).hasSingleBean(EventPublisherChannels::class.java)
                assertThat(channels(context)).isInstanceOf(RecordingEventPublisherChannels::class.java)
            }
    }

    @Test
    fun `the context closes the channels on shutdown`() {
        var publisher: ChannelEventPublisher? = null
        service.withPropertyValues(reportingOn).run { context ->
            publisher = channels(context).get("reporting") as ChannelEventPublisher
        }
        val failure = runCatching { publisher!!.publishWithResult("t1", "k", SampleEvent("x", "1")).get() }.exceptionOrNull()
        assertThat(failure?.cause).hasMessageContaining("stage=CHANNEL_UNAVAILABLE")
    }

    @Test
    fun `a failure handler that publishes through another channel works when it looks the catalog up lazily`() {
        service.withPropertyValues(
            reportingOn,
            "crypto.kafka.publisher.channels.reporting.producer.max-block-ms=200",
            "crypto.kafka.publisher.channels.dead-letters.enabled=false",
        ).withBean(LazyDeadLetterHandler::class.java).run { context ->
            assertThat(context).hasNotFailed()
            channels(context).get("reporting").publish("t1", "k", SampleEvent("x", "1"))
            val handler = context.getBean(LazyDeadLetterHandler::class.java)
            eventually { handler.republished.isNotEmpty() }
            assertThat(handler.republished.single().stage).isEqualTo(PublishFailureStage.DELIVERY_FAILED)
        }
    }

    @Test
    fun `a failure handler that takes the catalog in its constructor fails fast at startup (a bean cycle)`() {
        service.withBean(EagerDeadLetterHandler::class.java).run { context ->
            assertThat(context).hasFailed()
            assertThat(context.startupFailure).hasRootCauseInstanceOf(BeanCurrentlyInCreationException::class.java)
        }
    }

    /** The proposal's dead-letter use: the catalog needs the handlers at startup, so a handler resolves the catalog on first use. */
    class LazyDeadLetterHandler(private val channels: ObjectProvider<EventPublisherChannels>) : PublishFailureHandler {
        private val deadLetters by lazy { channels.getObject().get("dead-letters") }
        val republished = CopyOnWriteArrayList<PublishFailure>()

        override fun onFailure(failure: PublishFailure) {
            if (failure.channel == "dead-letters") return
            deadLetters.publishJson("${failure.topic}.dlt", failure.key, failure.payload ?: return)
            republished += failure
        }
    }

    class EagerDeadLetterHandler(channels: EventPublisherChannels) : PublishFailureHandler {
        private val deadLetters = channels.get("dead-letters")
        override fun onFailure(failure: PublishFailure) = deadLetters.publishJson("${failure.topic}.dlt", failure.key, failure.payload ?: "")
    }

    @Configuration(proxyBeanMethods = false)
    class TwoTemplates {
        @Bean
        fun firstTemplate(): KafkaTemplate<String, String> = KafkaTemplate(DefaultKafkaProducerFactory(mapOf<String, Any>("bootstrap.servers" to "127.0.0.1:1")))

        @Bean
        fun secondTemplate(): KafkaTemplate<String, String> = KafkaTemplate(DefaultKafkaProducerFactory(mapOf<String, Any>("bootstrap.servers" to "127.0.0.1:2")))
    }

    @Configuration(proxyBeanMethods = false)
    class PrimaryTemplate {
        @Bean
        @Primary
        fun primaryKafkaTemplate(): KafkaTemplate<String, String> = KafkaTemplate(
            DefaultKafkaProducerFactory(
                mapOf<String, Any>(
                    "bootstrap.servers" to "127.0.0.1:2",
                    "key.serializer" to StringSerializer::class.java,
                    "value.serializer" to StringSerializer::class.java,
                ),
            ),
        )
    }

    @Configuration(proxyBeanMethods = false)
    class OpaqueProducerFactory {
        @Bean
        fun kafkaTemplate(): KafkaTemplate<String, String> =
            KafkaTemplate(ProducerFactory<String, String> { MockProducer(true, StringSerializer(), StringSerializer()) })
    }
}
