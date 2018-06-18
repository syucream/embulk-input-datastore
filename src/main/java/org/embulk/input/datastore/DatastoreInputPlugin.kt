package org.embulk.input.datastore

import org.embulk.config.TaskReport
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigSource
import org.embulk.config.TaskSource
import org.embulk.spi.Exec
import org.embulk.spi.InputPlugin
import org.embulk.spi.PageOutput
import org.embulk.spi.Schema

class DatastoreInputPlugin : InputPlugin {

    override fun transaction(config: ConfigSource,
                    control: InputPlugin.Control): ConfigDiff {
        val task: PluginTask = config.loadConfig()

        val schema = task.columns.toSchema()
        val taskCount = 1  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control)
    }

    override fun resume(taskSource: TaskSource,
               schema: Schema, taskCount: Int,
               control: InputPlugin.Control): ConfigDiff {
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

        // Write your code here :)
        throw UnsupportedOperationException("DatastoreInputPlugin.run method is not implemented yet")
    }

    override fun guess(config: ConfigSource): ConfigDiff {
        return Exec.newConfigDiff()
    }
}
