package com.crp.system.libs.kafka.publisher.api

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

/**
 * Golden values: the contract's vector (GO_ReportingService/docs/contracts/reporting-events.md §2, checked there by
 * Go's `uuid.NewSHA1(uuid.NameSpaceOID, name)`) and Python's `uuid.uuid5(uuid.NAMESPACE_OID, name)`.
 */
class EventIdsTest {

    @Test
    fun `nameBased equals the reporting contract's golden vector`() {
        assertEquals("54ce620b-270c-53e3-aece-816f4f940dd7", EventIds.nameBased("AccountServices|player_registered|-7343247560384022447"))
    }

    @Test
    fun `nameBased equals an independent UUIDv5 implementation`() {
        mapOf(
            "AccountServices|player_registered|7" to "75364f0c-182d-52ee-a6b7-228a525fba1c",
            "a" to "d720e32d-4765-5177-b65b-42dd7f6e6445",
            "" to "0a68eb57-c88a-5f34-9e9d-27f85e68af4f",
            "NixxeRequestTransactionService|deposit_initiated|trk-0001" to "94c2b7ca-e472-57a5-b75b-88a4b1d7cc01",
            "KYCService|kyc_status_changed|Ελλάδα-ü-✓" to "fd873be9-65ca-5af6-b549-b49ec062b097", // UTF-8 name bytes
        ).forEach { (name, expected) -> assertEquals(expected, EventIds.nameBased(name), name) }
    }

    // ported from the services' ReportingEventPublisherTest (v2.3)
    @Test
    fun `eventIdIsStableV5`() {
        assertEquals(EventIds.nameBased("AccountServices|player_registered|7"), EventIds.nameBased("AccountServices|player_registered|7"))
        assertEquals('5', EventIds.nameBased("a")[14])
    }

    // ported from the services' ReportingEventPublisherTest (v2.3)
    @Test
    fun `timestampAlwaysHasMilliseconds`() {
        assertEquals("2026-10-06T08:15:30.000Z", EventIds.utcMillis(Instant.parse("2026-10-06T08:15:30Z")))
        assertEquals("2026-10-06T08:15:30.123Z", EventIds.utcMillis(Instant.parse("2026-10-06T08:15:30.123456Z")))
    }

    @Test
    fun `utcMillis is UTC whatever the default zone, and defaults to now`() {
        assertEquals("1970-01-01T00:00:00.000Z", EventIds.utcMillis(Instant.EPOCH))
        assertThat(EventIds.utcMillis()).matches("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z""")
    }

    @Test
    fun `nameBased sets the version 5 and RFC 4122 variant bits`() {
        val uuid = UUID.fromString(EventIds.nameBased("AccountServices|player_registered|7"))
        assertThat(uuid.version()).isEqualTo(5)
        assertThat(uuid.variant()).isEqualTo(2)
    }

    @Test
    fun `random is a fresh v4 each time`() {
        val ids = (1..100).map { UUID.fromString(EventIds.random()) }
        assertThat(ids).allMatch { it.version() == 4 && it.variant() == 2 }
        assertThat(ids.toSet()).hasSize(100)
    }
}
