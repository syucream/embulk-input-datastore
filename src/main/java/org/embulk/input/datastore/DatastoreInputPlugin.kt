package org.embulk.input.datastore

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.Timestamp
import com.google.cloud.datastore.*
import org.embulk.config.TaskReport
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigSource
import org.embulk.config.TaskSource
import org.embulk.spi.*
import org.embulk.spi.json.JsonParser
import org.embulk.spi.type.Types
import java.io.FileInputStream
import java.util.Base64

class DatastoreInputPlugin(doLogging: Boolean = true) : InputPlugin {
    // number of run() method calls
    private val TASK_COUNT = 1
    private val KEY_KEY = "__key__"

    private val logger = if (doLogging) {
        Exec.getLogger(javaClass)
    } else {
        null
    }
    private val b64encoder = Base64.getEncoder()
    private val jsonParser = JsonParser()

    override fun transaction(config: ConfigSource,
                             control: InputPlugin.Control): ConfigDiff {
        val task: PluginTask = config.loadConfig(PluginTask::class.java)

        // Support only 1 'json' type column
        val schema = Schema.builder()
                .add(task.jsonColumnName, Types.JSON)
                .build()

        return resume(task.dump(), schema, TASK_COUNT, control)
    }

    override fun resume(taskSource: TaskSource,
                        schema: Schema, taskCount: Int,
                        control: InputPlugin.Control): ConfigDiff {
        // XXX Unimplemented
        control.run(taskSource, schema, taskCount)
        return Exec.newConfigDiff()
    }

    override fun cleanup(taskSource: TaskSource,
                         schema: Schema, taskCount: Int,
                         successTaskReports: List<TaskReport>) {
    }

    override fun run(taskSource: TaskSource,
                     schema: Schema, taskIndex: Int,
                     output: PageOutput): TaskReport {
        val task = taskSource.loadTask(PluginTask::class.java)
        val allocator = task.getBufferAllocator()
        val pageBuilder = PageBuilder(allocator, schema, output)

        val query = Query
                .newGqlQueryBuilder(getGQLResultMode(task.gql), task.gql)
                .setAllowLiteral(true)
                .build()

        val datastore = createDatastoreClient(task)
        val col = pageBuilder.schema.getColumn(0)

        datastore.run(query)
                .forEach { entity ->
                    logger?.debug(entity.toString())

                    val json = when (entity) {
                        is FullEntity<*> -> entityToJsonObject(entity)
                        is ProjectionEntity -> entityToJsonObject(entity)
                        else -> null
                    }

                    json?.let {
                        logger?.debug(json)
                    } ?: run {
                        logger?.error("Unexpected result type")
                    }

                    pageBuilder.setJson(col, jsonParser.parse(json))
                    pageBuilder.addRecord()
                }

        pageBuilder.finish()

        return Exec.newTaskReport()
    }

    override fun guess(config: ConfigSource): ConfigDiff {
        // XXX Unimplemented
        return Exec.newConfigDiff()
    }

    /**
     * Setup datastore client
     *
     */
    private fun createDatastoreClient(task: PluginTask): Datastore {
        val cred = GoogleCredentials
                .fromStream(FileInputStream(task.jsonKeyfile))

        return DatastoreOptions.newBuilder()
                .setProjectId(task.projectId)
                .setCredentials(cred)
                .build()
                .service
    }

    /**
     *  Datastore entity -> JSON String
     *  e.g.) '{"name": "value", ...}'
     *
     */
    private fun entityToJsonObject(entity: BaseEntity<*>): String? {
        val fields = entity.names.flatMap { name ->
            val dsValue = entity.getValue<Value<Any>>(name)
            val strVal = valueToField(dsValue)
            strVal?.let { listOf("\"${name}\":${it}") } ?: listOf()
        }

        // Append KEY values if it exists
        val keyFields = getEntityKey(entity)?.let {
            listOf("\"${KEY_KEY}\":${it}")
        } ?: listOf()
        val decoratedFields = fields.plus(keyFields)

        return "{" + decoratedFields.joinToString() + "}"
    }

    /**
     *  Datastore property value list -> JSON Array
     *  e.g.) '[1,2,3]'
     *
     */
    private fun listToJsonArray(values: List<*>): String? {
        // NOTE: Do unsafe cast because of type erasure...
        val anyValues = values as? List<Value<Any>>

        anyValues?.let {
            val fields = it.flatMap { v ->
                val strVal = valueToField(v)
                strVal?.let { listOf(it) } ?: listOf()
            }
            return "[" + fields.joinToString() + "]"
        }

        return null
    }

    /**
     *  Datastore property value -> JSON field string
     *  e.g.) '"name":"value"'
     *
     */
    private fun valueToField(dsValue: Value<Any>): String? {
        return when (dsValue.type) {
            ValueType.BLOB -> "\"${b64encoder.encodeToString((dsValue.get() as Blob).toByteArray())}\""
            ValueType.BOOLEAN -> (dsValue.get() as Boolean).toString()
            ValueType.DOUBLE -> (dsValue.get() as Double).toString()
            ValueType.ENTITY -> entityToJsonObject(dsValue.get() as BaseEntity<*>)
            ValueType.KEY -> (dsValue.get() as Key).toString()
            ValueType.LAT_LNG -> (dsValue.get() as LatLngValue).toString()
            ValueType.LIST -> listToJsonArray(dsValue.get() as List<*>)
            ValueType.LONG -> (dsValue.get() as Long).toString()
            ValueType.NULL -> "null"
            ValueType.RAW_VALUE -> (dsValue.get() as RawValue).toString()
            ValueType.STRING -> "\"${dsValue.get() as String}\""
            ValueType.TIMESTAMP -> "\"${dsValue.get() as Timestamp}\""
            else -> null // NOTE: unexpected or unsupported type
        }
    }

    /**
     *  Check the GQL will return FullEntity or ProjectionEntity
     *  NOTE: GQL accepts '*' at only after 'SELECT'
     *    https://cloud.google.com/datastore/docs/reference/gql_reference
     *
     */
    private fun getGQLResultMode(gql: String): Query.ResultType<*> {
        return if (gql.indexOf("*") >= 0) {
            // Lookup all columns
            Query.ResultType.ENTITY
        } else {
            // Lookup a part of columns
            Query.ResultType.PROJECTION_ENTITY
        }
    }

    /**
     * Get KEY info string (with calling private method ...)
     */
    private fun getEntityKey(entity: BaseEntity<*>): String? {
        if (entity.hasKey()) {
            val key = entity.key

            // NOTE: getLeaf() is not public ...
            val mirror = BaseKey::class.java.getDeclaredMethod("getLeaf")
            mirror.isAccessible = true
            val pe = mirror.invoke(key) as PathElement

            return "{\"kind\":\"${pe.kind}\",\"id\":\"${pe.id}\",\"name\":\"${pe.name}\"}"
        } else {
            return null
        }
    }
}
