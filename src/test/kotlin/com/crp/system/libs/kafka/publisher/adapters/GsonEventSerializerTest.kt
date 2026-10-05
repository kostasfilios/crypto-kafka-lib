package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.spring.SerializerNaming
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class GsonEventSerializerTest {
    data class SampleEvent(val eventType: String, val playerId: String)
    data class WithNulls(val eventId: String, val closingBalance: java.math.BigDecimal?, val failureReason: String?)
    data class Html(val note: String)

    private val snake = GsonEventSerializer(SerializerNaming.SNAKE_CASE)
    private val identity = GsonEventSerializer(SerializerNaming.IDENTITY)

    // the reporting copy's publishDoesNotRunOnCallerThreadAndUsesSnakeCase JSON, unchanged
    @Test
    fun `snake_case naming gives the reporting contract's field names`() {
        assertEquals(
            """{"event_type":"player_registered","player_id":"-7343247560384022447"}""",
            snake.serialize(SampleEvent("player_registered", "-7343247560384022447")),
        )
    }

    @Test
    fun `identity naming keeps the Kotlin field names`() {
        assertEquals("""{"eventType":"player_registered","playerId":"7"}""", identity.serialize(SampleEvent("player_registered", "7")))
    }

    @Test
    fun `nulls are kept, in both namings`() {
        assertEquals("""{"event_id":"e1","closing_balance":null,"failure_reason":null}""", snake.serialize(WithNulls("e1", null, null)))
        assertEquals("""{"eventId":"e1","closingBalance":null,"failureReason":null}""", identity.serialize(WithNulls("e1", null, null)))
    }

    @Test
    fun `HTML characters are not escaped`() {
        assertEquals("""{"note":"<b>a & b = 'c'</b>"}""", snake.serialize(Html("<b>a & b = 'c'</b>")))
        assertThat(identity.serialize(Html("<>&='"))).isEqualTo("""{"note":"<>&='"}""")
    }

    @Test
    fun `decimals keep their scale`() {
        assertThat(snake.serialize(WithNulls("e1", java.math.BigDecimal("90.00"), ""))).isEqualTo("""{"event_id":"e1","closing_balance":90.00,"failure_reason":""}""")
    }
}
