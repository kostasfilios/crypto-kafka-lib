package com.crp.system.libs.kafka.publisher.adapters

import com.crp.system.libs.kafka.publisher.api.EventSerializer
import com.crp.system.libs.kafka.publisher.spring.SerializerNaming
import com.google.gson.FieldNamingPolicy
import com.google.gson.Gson
import com.google.gson.GsonBuilder

/** Gson as the services' reporting publisher configured it: the channel's naming, nulls kept, no HTML escaping. */
internal class GsonEventSerializer(naming: SerializerNaming) : EventSerializer {
    private val gson: Gson = GsonBuilder()
        .setFieldNamingPolicy(
            when (naming) {
                SerializerNaming.SNAKE_CASE -> FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES
                SerializerNaming.IDENTITY -> FieldNamingPolicy.IDENTITY
            },
        )
        .serializeNulls() // contract: every field is always present, optional ones as null
        .disableHtmlEscaping()
        .create()

    override fun serialize(event: Any): String = gson.toJson(event)
}
