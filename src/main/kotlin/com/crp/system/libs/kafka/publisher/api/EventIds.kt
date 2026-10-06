package com.crp.system.libs.kafka.publisher.api

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * Event ids and timestamps of the reporting contract (`GO_ReportingService/docs/contracts/reporting-events.md` §1-2).
 * Ported unchanged from the services' `ReportingEventIds` v2.3: `v5` is [nameBased], `timestamp` is [utcMillis].
 */
object EventIds {
    private val namespaceOid: UUID = UUID.fromString("6ba7b812-9dad-11d1-80b4-00c04fd430c8")

    /** UUIDv5 (SHA-1, RFC 4122) in the OID namespace over [name], e.g. "<source_service>|<event_type>|<natural id>". */
    fun nameBased(name: String): String {
        val digest = MessageDigest.getInstance("SHA-1")
        digest.update(ByteBuffer.allocate(16).putLong(namespaceOid.mostSignificantBits).putLong(namespaceOid.leastSignificantBits).array())
        digest.update(name.toByteArray(Charsets.UTF_8))
        val hash = digest.digest()
        hash[6] = ((hash[6].toInt() and 0x0f) or 0x50).toByte()
        hash[8] = ((hash[8].toInt() and 0x3f) or 0x80).toByte()
        val buffer = ByteBuffer.wrap(hash, 0, 16)
        return UUID(buffer.long, buffer.long).toString()
    }

    /** UUIDv4. */
    fun random(): String = UUID.randomUUID().toString()

    // Always three fraction digits (ISO_INSTANT drops ".000" on whole seconds); the contract wants e.g. 2026-10-06T08:15:30.000Z
    private val millisecondsUtc: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    /** RFC 3339 UTC with milliseconds, e.g. 2026-10-06T08:15:30.123Z. */
    fun utcMillis(instant: Instant = Instant.now()): String = millisecondsUtc.format(instant)
}
