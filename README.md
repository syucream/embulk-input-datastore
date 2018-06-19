# Datastore input plugin for Embulk

[![Gem Version](https://badge.fury.io/rb/embulk-input-datastore.svg)](https://badge.fury.io/rb/embulk-input-datastore)

A embulk input plugin fetches Cloud Datastore entities.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **json_keyfile**: A path to JSON keyfile. (string, required)
- **gql**: A GQL fetches to Cloud Datastore (string, required)
- **json_column_name**: description (string, default: `"record"`)

## Example

```yaml
in:
  type: datastore
  json_keyfile: example1
  gql: "SELECT * FROM myKind WHERE myProp >= 100 AND myProp < 200"
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## NOTE

Currently this plugin aggregates fetched entities to 1 'json' type column.

