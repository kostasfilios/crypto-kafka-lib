package com.crp.system.libs.kafka.publisher.api

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class PublishFailureTest {
    private val failure = PublishFailure(
        "reporting", "t1", "42", PublishFailureStage.DELIVERY_FAILED, 1, true, null,
        """{"player_id":"42","email":"player@example.com"}""", Instant.parse("2026-10-05T12:00:00Z"),
    )

    @Test
    fun `toString shows the payload's length, never the payload`() {
        assertThat(failure.toString())
            .isEqualTo(
                "PublishFailure(channel=reporting, topic=t1, key=42, stage=DELIVERY_FAILED, attempt=1, retriable=true, " +
                    "cause=null, payload=<47 chars>, occurredAt=2026-10-05T12:00:00Z)",
            )
            .doesNotContain("player@example.com")
    }

    @Test
    fun `toString says null for a failure without a payload`() {
        assertThat(failure.copy(payload = null).toString()).contains("payload=null,")
    }

    @Test
    fun `equality still covers the payload`() {
        assertThat(failure).isNotEqualTo(failure.copy(payload = "{}")).isEqualTo(failure.copy())
    }
}
