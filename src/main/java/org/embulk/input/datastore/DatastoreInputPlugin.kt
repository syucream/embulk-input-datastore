package org.embulk.input.datastore

import com.google.auth.oauth2.GoogleCredentials
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
    val TASK_COUNT = 1

    var datastore: Datastore? = null

    override fun transaction(config: ConfigSource,
                    control: InputPlugin.Control): ConfigDiff {
        val task: PluginTask = config.loadConfig(PluginTask::class.java)

        // Support only 1 'json' type column
        val schema = Schema.builder()
                .add(task.jsonColumnName, Types.JSON)
                .build()

        // Setup datastore client
        val cred = GoogleCredentials
                .fromStream(FileInputStream(task.jsonKeyfile))
        this.datastore = DatastoreOptions.newBuilder()
                .setCredentials(cred)
                .build()
                .service

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

        datastore?.let {
            val col = pageBuilder.schema.getColumn(0)

            it.run(query)
                    .forEach { entity ->
                        val msgpackValue = ValueFactory.newString(entity.toString())
                        pageBuilder.setJson(col, msgpackValue)
                        pageBuilder.addRecord()
                    }

            pageBuilder.finish()
        }

        throw UnsupportedOperationException("DatastoreInputPlugin.run method is not implemented yet")
    }

    override fun guess(config: ConfigSource): ConfigDiff {
        // XXX Unimplemented
        return Exec.newConfigDiff()
    }
}
