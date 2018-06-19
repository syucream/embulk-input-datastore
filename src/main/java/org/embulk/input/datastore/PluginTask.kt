package org.embulk.input.datastore

import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigInject
import org.embulk.config.Task
import org.embulk.spi.BufferAllocator

interface PluginTask : Task {
    @get:Config("json_keyfile")
    val jsonKeyfile: String

    @get:Config("gql")
    val gql: String

    @get:Config("json_column_name")
    @get:ConfigDefault("\"record\"")
    val jsonColumnName: String

    @ConfigInject
    fun getBufferAllocator(): BufferAllocator
}
