package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublisherMetrics
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier

/**
 * Micrometer adapter, created only when the service has a `MeterRegistry` bean (Micrometer is compileOnly in the lib):
 * - `kafka.publisher.delivered` timer (channel, topic): enqueue to acknowledgement;
 * - `kafka.publisher.failed` counter (channel, topic, stage);
 * - `kafka.publisher.queue.depth` gauge (channel).
 */
internal class MicrometerPublisherMetrics(private val registry: MeterRegistry) : PublisherMetrics {
    private val deliveredTimers = ConcurrentHashMap<String, ConcurrentHashMap<String, Timer>>()
    private val failedCounters = ConcurrentHashMap<String, ConcurrentHashMap<String, Counter>>()

    override fun delivered(channel: String, topic: String, latency: Duration) {
        val timers = deliveredTimers.computeIfAbsent(channel) { ConcurrentHashMap() }
        val timer = timers[topic] ?: timers.computeIfAbsent(topic) {
            Timer.builder("kafka.publisher.delivered").tag("channel", channel).tag("topic", topic).register(registry)
        }
        timer.record(latency)
    }

    override fun failed(channel: String, topic: String, stage: PublishFailureStage) {
        val counters = failedCounters.computeIfAbsent(channel) { ConcurrentHashMap() }
        val name = "$topic\u0000${stage.name}"
        val counter = counters[name] ?: counters.computeIfAbsent(name) {
            Counter.builder("kafka.publisher.failed").tag("channel", channel).tag("topic", topic).tag("stage", stage.name).register(registry)
        }
        counter.increment()
    }

    override fun queueDepth(channel: String, depth: () -> Int) {
        Gauge.builder("kafka.publisher.queue.depth", Supplier<Number> { depth() })
            .tag("channel", channel)
            .strongReference(true)
            .register(registry)
    }
}
