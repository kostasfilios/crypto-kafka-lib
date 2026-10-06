package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.RetryPolicy
import com.crp.system.libs.kafka.publisher.spring.RetrySettings
import java.time.Duration
import kotlin.math.pow

/** initial-backoff x multiplier^(n-2) before attempt n, capped at max-backoff; null after max-attempts. */
internal class ExponentialRetryPolicy(private val settings: RetrySettings) : RetryPolicy {
    override fun delayBeforeAttempt(nextAttempt: Int): Duration? {
        if (nextAttempt > settings.maxAttempts) return null
        val millis = settings.initialBackoff.toMillis() * settings.multiplier.pow(nextAttempt - 2)
        return Duration.ofMillis(millis.toLong().coerceAtMost(settings.maxBackoff.toMillis()))
    }
}
