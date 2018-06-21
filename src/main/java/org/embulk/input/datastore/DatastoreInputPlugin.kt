package org.embulk.input.datastore

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.Timestamp
import com.google.cloud.datastore.*
import org.embulk.config.TaskReport
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigSource
import org.embulk.config.TaskSource
import org.embulk.spi.*
import org.embulk.spi.type.Types
import org.msgpack.value.ValueFactory
import java.io.FileInputStream

class DatastoreInputPlugin : InputPlugin {
    // number of run() method calls
    private val TASK_COUNT = 1

    private val logger = Exec.getLogger(javaClass)

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
                .newGqlQueryBuilder(Query.ResultType.ENTITY, task.gql)
                .build()

        val datastore = createDatastoreClient(task)
        val col = pageBuilder.schema.getColumn(0)

        datastore.run(query)
                .forEach { entity ->
                    logger.debug(entity.toString())

                    // TODO separate/simplify generating JSON
                    val pairs = entity.names.flatMap { name ->
                        val dsValue = entity.getValue<Value<Any>>(name)
                        val strVal: String? = when (dsValue.type) {
                            ValueType.BLOB -> (dsValue.get() as ByteArray).toString()
                            ValueType.BOOLEAN -> (dsValue.get() as Boolean).toString()
                            ValueType.DOUBLE -> (dsValue.get() as Double).toString()
                            ValueType.LONG -> (dsValue.get() as Long).toString()
                            ValueType.STRING -> "\"${dsValue.get() as String}\""
                            ValueType.TIMESTAMP -> (dsValue.get() as Timestamp).toString()
                            else -> null // NOTE, TODO: LIST, ENTITY, ... is still unsupported
                        }
                        strVal?.let { listOf(Pair<String, String>(name, it)) } ?: listOf()
                    }
                    val json = "{" + pairs.map { pair ->
                        "\"${pair.first}\": ${pair.second}"
                    }.joinToString() + "}"

                    val msgpackValue = ValueFactory.newString(json)
                    pageBuilder.setJson(col, msgpackValue)
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
}
