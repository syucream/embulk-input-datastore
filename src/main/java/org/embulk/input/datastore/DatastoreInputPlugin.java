package org.embulk.input.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.slf4j.Logger;
import java.util.List;

public class DatastoreInputPlugin
        implements InputPlugin
{
    private final Logger log = Exec.getLogger(DatastoreInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // TODO validation

        Schema schema = Schema.builder().build();

        return resume(task.dump(), schema, 1, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        ConfigDiff configDiff = Exec.newConfigDiff();
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successCommitReports)
    {
        // do nothing
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);
        final Column column = pageBuilder.getSchema().getColumns().get(0);

        // TODO load input

        pageBuilder.finish();
        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private Datastore connect(final PluginTask task)
    {
        if (!task.getJsonKeyfile().isPresent()) {
            throw new ConfigException("'hosts' option's value is required but empty");
        }
        String credPath = task.getJsonKeyfile().get();

        return DatastoreOptions
          .newBuilder()
          .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credPath)))
          .build()
          .getService();
    }
}
