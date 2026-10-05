package com.crp.system.libs.kafka.publisher.spring

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * Settings bound from `crypto.kafka.publisher.*`. Kotlin defaults apply to missing keys.
 *
 * `ignoreInvalidFields`: a value that does not convert (an enum typo, a malformed duration) must not stop the
 * service (AD10). The auto-configuration re-checks each channel strictly and switches a channel with such a value
 * off with an ERROR.
 */
@ConfigurationProperties(prefix = EventPublisherProperties.PREFIX, ignoreInvalidFields = true)
data class EventPublisherProperties(
    /** How often the failure and drop summaries are logged. */
    val summaryInterval: Duration = Duration.ofSeconds(10),
    val channels: Map<String, ChannelSettings> = emptyMap(),
) {
    companion object {
        const val PREFIX = "crypto.kafka.publisher"
    }
}

data class ChannelSettings(
    val enabled: Boolean = false,
    val serializer: SerializerNaming = SerializerNaming.SNAKE_CASE,
    val lanes: Int = 2,
    /** Split evenly across the lanes. */
    val queueCapacity: Int = 10_000,
    val backpressure: BackpressureMode = BackpressureMode.DROP,
    /** BLOCK_WITH_TIMEOUT only. */
    val blockTimeout: Duration = Duration.ofMillis(100),
    val ordering: OrderingMode = OrderingMode.PER_KEY,
    val retry: RetrySettings = RetrySettings(),
    val producer: ProducerSettings = ProducerSettings(),
    /** Checked once at startup; never created. */
    val topics: List<String> = emptyList(),
    val shutdownTimeout: Duration = Duration.ofSeconds(5),
)

data class RetrySettings(
    /** 1 = only the producer's own retries. */
    val maxAttempts: Int = 1,
    val initialBackoff: Duration = Duration.ofMillis(200),
    val multiplier: Double = 2.0,
    val maxBackoff: Duration = Duration.ofSeconds(5),
)

data class ProducerSettings(
    val maxBlockMs: Long = 2_000,
    val bufferMemory: Long = 8L * 1024 * 1024,
    val lingerMs: Int = 5,
    val requestTimeoutMs: Int = 10_000,
    /** Must be >= linger + request timeout. */
    val deliveryTimeoutMs: Int = 30_000,
    val acks: String = "all",
    val compressionType: String = "none",
    /** Any other ProducerConfig key, applied last. */
    val extra: Map<String, String> = emptyMap(),
)

enum class SerializerNaming { SNAKE_CASE, IDENTITY }

enum class BackpressureMode { DROP, CALLER_RUNS, BLOCK_WITH_TIMEOUT }

enum class OrderingMode { PER_KEY, NONE }
