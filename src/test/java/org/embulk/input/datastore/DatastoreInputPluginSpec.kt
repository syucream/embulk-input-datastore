package org.embulk.input.datastore

import com.google.cloud.datastore.Entity
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.isAccessible
import kotlin.test.assertEquals

object DatastoreInputPluginSpec: Spek({
    describe("entityToJsonObject()") {
        val plugin = DatastoreInputPlugin()

        val method = plugin::class.memberFunctions.find { it.name == "entityToJsonObject"}
        val accesibleMethod = method!!.let {
            it.isAccessible = true
            it
        }

        on("bool property value") {
            val entity = Entity.newBuilder().set("boolProp", true).build()
            val json = accesibleMethod.call(plugin, entity) as String

            it("should be converted bool json value") {
                assertEquals("{\"boolProp\":true}", json)
            }
        }
    }
})
