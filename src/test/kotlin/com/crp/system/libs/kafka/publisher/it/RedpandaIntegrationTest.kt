package com.crp.system.libs.kafka.publisher.it

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.DELIVERY_FAILED
import com.crp.system.libs.kafka.publisher.api.PublishResult
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.spring.ConfiguredEventPublisherChannels
import com.crp.system.libs.kafka.publisher.spring.EventPublisherProperties
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * Against a real broker: `KAFKA_PUBLISHER_TEST_BOOTSTRAP=localhost:19093 ./gradlew test --tests '*RedpandaIntegrationTest'`.
 * Skipped when the variable is unset. Creates its own `event-publisher-test-<run>*` topics and deletes them at the end;
 * never changes the broker's configuration.
 */
@EnabledIfEnvironmentVariable(named = "KAFKA_PUBLISHER_TEST_BOOTSTRAP", matches = ".+")
class RedpandaIntegrationTest {

    companion object {
        private const val PREFIX = "event-publisher-test-"
        private val run = UUID.randomUUID().toString().substring(0, 8)
        private val topic = "$PREFIX$run"
        private val drainTopic = "$PREFIX$run-drain"
        private val bootstrap: String get() = System.getenv("KAFKA_PUBLISHER_TEST_BOOTSTRAP")
        private lateinit var admin: Admin

        @JvmStatic
        @BeforeAll
        fun createTopic() {
            admin = AdminClient.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap))
            admin.createTopics(listOf(NewTopic(topic, 3, 1.toShort()), NewTopic(drainTopic, 3, 1.toShort()))).all().get(30, TimeUnit.SECONDS)
        }

        @JvmStatic
        @AfterAll
        fun deleteTopics() {
            val mine = admin.listTopics().names().get(30, TimeUnit.SECONDS).filter { it.startsWith(PREFIX) && it.contains(run) }
            if (mine.isNotEmpty()) admin.deleteTopics(mine).all().get(30, TimeUnit.SECONDS)
            admin.close(Duration.ofSeconds(5))
        }
    }

    private fun channels(name: String, settings: ChannelSettings) = ConfiguredEventPublisherChannels.create(
        properties = EventPublisherProperties(channels = mapOf(name to settings)),
        sharedProducerConfig = mapOf("bootstrap.servers" to bootstrap),
        extraFailureHandlers = emptyList(),
        metrics = PublisherMetrics.None,
        applicationName = "event-publisher-it-$run",
        clock = Clock.systemUTC(),
    )

    @Test
    fun `keyed records reach the broker with their headers, one partition and in order per key`() {
        val channels = channels("it", ChannelSettings(enabled = true, lanes = 2, topics = listOf(topic)))
        try {
            val publisher = channels.get("it")
            val sent = ArrayList<Triple<String, Int, CompletableFuture<PublishResult>>>()
            var slowestCallMillis = 0L
            for (seq in 1..5) {
                for (player in listOf("player-1", "player-2", "player-3")) {
                    val started = System.nanoTime()
                    val headers = mapOf("source_service" to "event-publisher-it", "trace" to "αβγ-$seq")
                    sent += Triple(player, seq, publisher.publishWithResult(topic, player, SampleEvent("player_registered", player, seq), headers))
                    slowestCallMillis = maxOf(slowestCallMillis, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
                }
            }
            val results = sent.map { (player, seq, future) -> Triple(player, seq, future.get(30, TimeUnit.SECONDS)) }
            val records = consume(topic, expected = results.size)

            assertThat(slowestCallMillis).isLessThan(500)
            assertThat(records).hasSize(results.size)
            results.forEach { (player, seq, result) ->
                val record = records.single { it.partition() == result.partition && it.offset() == result.offset }
                assertThat(record.key()).isEqualTo(player)
                assertThat(record.value()).isEqualTo("""{"event_type":"player_registered","player_id":"$player","seq":$seq}""")
                assertThat(record.headers().associate { it.key() to String(it.value(), Charsets.UTF_8) })
                    .isEqualTo(mapOf("source_service" to "event-publisher-it", "trace" to "αβγ-$seq"))
                assertThat(result.topic).isEqualTo(topic)
                assertThat(result.key).isEqualTo(player)
                assertThat(result.attempts).isEqualTo(1)
            }
            results.groupBy { it.first }.forEach { (player, byPlayer) ->
                assertThat(byPlayer.map { it.third.partition }.distinct()).describedAs(player).hasSize(1)
                assertThat(byPlayer.sortedBy { it.second }.map { it.third.offset }).describedAs(player).isSorted
            }
        } finally {
            channels.close()
        }
    }

    @Test
    fun `the startup topic check reports only the missing topic, and creates nothing`() {
        val missing = "$PREFIX$run-never-created"
        LogCapture(ConfiguredEventPublisherChannels::class.java).use { logs ->
            val channels = channels("check", ChannelSettings(enabled = true, topics = listOf(topic, missing)))
            try {
                eventually(Duration.ofSeconds(15)) { logs.lines(Level.ERROR).isNotEmpty() || logs.lines(Level.WARN, "kafka_publisher_topic_check_failed").isNotEmpty() }
                assertThat(logs.lines(Level.ERROR)).containsExactly("kafka_publisher_topic_missing channel=check topic=$missing")
            } finally {
                channels.close()
            }
        }
        assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).doesNotContain(missing)
    }

    @Test
    fun `close delivers what was queued before it closes the producer`() {
        val channels = channels("drain", ChannelSettings(enabled = true, lanes = 2))
        val futures = (1..200).map { channels.get("drain").publishWithResult(drainTopic, "drain-$it", SampleEvent("x", "drain-$it", it)) }
        channels.close()
        futures.forEach { assertThat(it.get(30, TimeUnit.SECONDS).offset).isGreaterThanOrEqualTo(0) }
    }

    @Test
    fun `a missing topic costs the caller nothing and ends as DELIVERY_FAILED after max-block-ms`() {
        assumeFalse(
            brokerAutoCreatesTopics(),
            "this broker auto-creates topics (Redpanda auto_create_topics_enabled=true), so a producer never sees a missing topic here; " +
                "RealProducerMissingTopicTest covers the same producer path against an unreachable broker",
        )
        val channels = channels("missing", ChannelSettings(enabled = true))
        try {
            val started = System.nanoTime()
            val result = channels.get("missing").publishWithResult("$PREFIX$run-missing", "42", SampleEvent("x", "42"))
            val callerMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            val failure = result.awaitFailure()

            assertThat(callerMillis).isLessThan(500)
            assertThat(failure.stage).isEqualTo(DELIVERY_FAILED)
            assertThat(failure.cause).hasMessageContaining("not present in metadata after 2000 ms")
        } finally {
            channels.close()
        }
    }

    /** Probe: ask a plain producer for a fresh topic's partitions. A broker that auto-creates answers (the topic now exists; deleted at the end). */
    private fun brokerAutoCreatesTopics(): Boolean =
        KafkaProducer<String, String>(
            mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.MAX_BLOCK_MS_CONFIG to 3_000,
            ),
        ).use { producer ->
            try {
                producer.partitionsFor("$PREFIX$run-probe").isNotEmpty()
            } catch (e: TimeoutException) {
                false
            }
        }

    private fun consume(topic: String, expected: Int): List<ConsumerRecord<String, String>> =
        KafkaConsumer<String, String>(
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ),
        ).use { consumer ->
            val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            val records = ArrayList<ConsumerRecord<String, String>>()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            while (records.size < expected && System.nanoTime() < deadline) records += consumer.poll(Duration.ofMillis(500))
            records
        }
}
