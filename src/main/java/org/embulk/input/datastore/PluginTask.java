package org.embulk.input.datastore;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.SchemaConfig;

import javax.validation.constraints.Min;

import java.util.List;
import java.util.Map;

public interface PluginTask
        extends Task
{
    @Config("json_keyfile")
    @ConfigDefault("null")
    Optional<String> getJsonKeyfile();

    @Config("query")
    @ConfigDefault("null")
    Optional<String> getQuery();

    @ConfigInject
    BufferAllocator getBufferAllocator();
}
