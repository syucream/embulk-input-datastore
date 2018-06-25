package org.embulk.input.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.isAccessible
import kotlin.test.assertEquals
import java.util.Base64

object DatastoreInputPluginSpec : Spek({
    val b64Encoder = Base64.getEncoder()

    describe("entityToJsonObject()") {
        val plugin = DatastoreInputPlugin(false)

        val method = plugin::class.memberFunctions.find { it.name == "entityToJsonObject" }
        val accesibleMethod = method!!.let {
            it.isAccessible = true
            it
        }

        on("single property values") {
            val testcases = listOf<Pair<FullEntity<IncompleteKey>, String>>(
                    Pair(
                            Entity.newBuilder().set("blobProp", Blob.copyFrom("test".toByteArray())).build(),
                            "{\"blobProp\":\"${b64Encoder.encodeToString("test".toByteArray())}\"}"
                    ),
                    Pair(
                            Entity.newBuilder().set("boolProp", true).build(),
                            "{\"boolProp\":true}"
                    ),
                    Pair(
                            Entity.newBuilder().set("doubleProp", 1.1).build(),
                            "{\"doubleProp\":1.1}"
                    ),
                    Pair(
                            Entity.newBuilder().set("entityProp", FullEntity.newBuilder().set("name1", 1).build()).build(),
                            "{\"entityProp\":{\"name1\":1}}"
                    ),
                    Pair(
                            Entity.newBuilder().set("listProp", 1, 2, 3).build(),
                            "{\"listProp\":[1, 2, 3]}"
                    ),
                    Pair(
                            Entity.newBuilder().set("nullProp", NullValue.of()).build(),
                            "{\"nullProp\":null}"
                    ),
                    Pair(
                            Entity.newBuilder().set("longProp", 1).build(),
                            "{\"longProp\":1}"
                    ),
                    Pair(
                            Entity.newBuilder().set("stringProp", "test").build(),
                            "{\"stringProp\":\"test\"}"
                    ),
                    Pair(
                            Entity.newBuilder().set("timestampProp", Timestamp.MIN_VALUE).build(),
                            "{\"timestampProp\":\"0001-01-01T00:00:00Z\"}"
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

        on("combinational property values") {
            val testcases = listOf<Pair<FullEntity<IncompleteKey>, String>>(
                    Pair(
                            Entity.newBuilder()
                                    .set("blobProp", Blob.copyFrom("test".toByteArray()))
                                    .set("boolProp", true)
                                    .set("doubleProp", 1.1)
                                    .set("entityProp", FullEntity.newBuilder().set("name1", 1).build())
                                    .set("listProp", 1, 2, 3)
                                    .set("nullProp", NullValue.of())
                                    .set("longProp", 1)
                                    .set("stringProp", "test")
                                    .set("timestampProp", Timestamp.MIN_VALUE)
                                    .build(),
                            "{" +
                                    "\"blobProp\":\"${b64Encoder.encodeToString("test".toByteArray())}\", " +
                                    "\"boolProp\":true, " +
                                    "\"doubleProp\":1.1, " +
                                    "\"entityProp\":{\"name1\":1}, " +
                                    "\"listProp\":[1, 2, 3], " +
                                    "\"longProp\":1, " +
                                    "\"nullProp\":null, " +
                                    "\"stringProp\":\"test\", " +
                                    "\"timestampProp\":\"0001-01-01T00:00:00Z\"" +
                                    "}"
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

    describe("getGQLResultMode()") {
        val plugin = DatastoreInputPlugin(false)

        val method = plugin::class.memberFunctions.find { it.name == "getGQLResultMode" }
        val accesibleMethod = method!!.let {
            it.isAccessible = true
            it
        }

        on("SELECT * query") {
            it("should returns FullEntity") {
                val resultMode = accesibleMethod.call(plugin, "SELECT * FROM myKind") as Query.ResultType<*>
                assertEquals(Query.ResultType.ENTITY, resultMode)
            }
        }

        on("SELECT non-* query") {
            it("should returns ProjecttionEntity") {
                val resultMode = accesibleMethod.call(plugin, "SELECT myProp FROM myKind") as Query.ResultType<*>
                assertEquals(Query.ResultType.PROJECTION_ENTITY, resultMode)
            }
        }
    }
})
