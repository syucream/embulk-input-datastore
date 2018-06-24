package org.embulk.input.datastore

import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.FullEntity
import com.google.cloud.datastore.IncompleteKey
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.isAccessible
import kotlin.test.assertEquals

object DatastoreInputPluginSpec : Spek({
    describe("entityToJsonObject()") {
        val plugin = DatastoreInputPlugin(false)

        val method = plugin::class.memberFunctions.find { it.name == "entityToJsonObject" }
        val accesibleMethod = method!!.let {
            it.isAccessible = true
            it
        }

        on("property values") {
            val testcases = listOf<Pair<FullEntity<IncompleteKey>, String>>(
                    Pair(
                            Entity.newBuilder().set("boolProp", true).build(),
                            "{\"boolProp\":true}"
                    ),
                    Pair(
                            Entity.newBuilder().set("doubleProp", 1.1).build(),
                            "{\"doubleProp\":1.1}"
                    ),
                    Pair(
                            Entity.newBuilder().set("listProp", 1, 2, 3).build(),
                            "{\"listProp\":[1, 2, 3]}"
                    ),
                    Pair(
                            Entity.newBuilder().set("longProp", 1).build(),
                            "{\"longProp\":1}"
                    ),
                    Pair(
                            Entity.newBuilder().set("stringProp", "test").build(),
                            "{\"stringProp\":\"test\"}"
                    )
            )

            it("should be converted json value") {
                testcases.forEach { pair ->
                    val entity = pair.first
                    val expected = pair.second

                    val actual = accesibleMethod.call(plugin, entity) as String
                    assertEquals(expected, actual)
                }
            }
        }
    }
})
