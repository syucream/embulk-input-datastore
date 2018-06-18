package org.embulk.input.datastore

import org.embulk.config.Config
import org.embulk.config.Task

interface PluginTask : Task {
    @get:Config("json_keyfile")
    val jsonKeyfile: String

    @get:Config("gql")
    val gql: String
}
