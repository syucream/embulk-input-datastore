package org.embulk.input.datastore

import com.google.cloud.datastore.Entity
import kotlin.test.Test
import kotlin.test.assertEquals

class TestDatastoreInputPlugin {
    @Test
    fun entityToJsonObject() {
        val plugin = DatastoreInputPlugin()

        val method = plugin::class.java.getDeclaredMethod("entityToJsonObject")
        method.isAccessible

        val entity = Entity.newBuilder().set("boolProp", true).build()
        val json = method.invoke(entity) as String

        assertEquals("{\"boolProp\": true}", json)
    }
}
