Embulk::JavaPlugin.register_input(
  "datastore", "org.embulk.input.datastore.DatastoreInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
