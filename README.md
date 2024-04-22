mmmbop
======
An ultra-fast and resilient tool for migrating MongoDB data -> PostgreSQL.

It is specifically designed to be able to deal with migrating exceptionally large
document stores which requires checkpointing, idempotency, and parallelism.

If you need to migrate a small amount of data - there are probably better tools
suited for that task. If you need to migrate 10TB's of data over the course of 
a few days, this is the tool for you.

## Install
Download the latest release from the [releases page](github.com/dselans/mmmbop/releases),
put the binary in your path, `chmod +x` it and you're good to go.

## Usage
```
Usage: mmmbop -h
    -h, --help
            Show this help message
    -v, --version
            Show the version of the program
    -c, --config cfg.toml (required)
            Path to the configuration file
    -d, --dry-run
            Run the migration without writing to the destination database
    -m, --migrate
            Run the migration
```

## Configuration
The configuration file is a TOML file that looks like this:

```toml
[config]
## Log level (default: info)
# log_level = "info"

## Number of workers we will use for this migration (default: num cores)
# num_workers = 4

## Number of documents we will fetch from the source database at a time (default: 1000)
# batch_size = 1000

## Where we will write checkpointing info to (default: mongo-migrate-checkpoint.json)
# checkpoint_file = "mongo-migrate-checkpoint.json"

## How often we will dump checkpointing info to disk (default: 1s)
# checkpoint_interval = "1s"

## Field used for checkpointing (default: _id)
# checkpoint_field = "_id"

[source]
dsn = "mongodb://localhost:27017"
database = "source_db"

[destination]
dsn = "postgres://user:password@localhost:5432/dbname"
database = "destination_db"

[mapping]
foo_mapping = [
    { src = "SOURCE_TABLE_NAME.foo", dst = "DST_TABLE_NAME.bar", conv = "int", required = true},
    { src = "SOURCE_TABLE_NAME.baz", dst = "DST_TABLE_NAME.qux", conv = "string" },
    { src = "SOURCE_TABLE_NAME.quux", dst = "DST_TABLE_NAME.corge", conv = "float" },
    { src = "SOURCE_TABLE_NAME.grault", dst = "DST_TABLE_NAME.garply", conv = "bool" },
    { src = "SOURCE_TABLE_NAME.waldo", dst = "DST_TABLE_NAME.fred", conv = "date" },
    { src = "SOURCE_TABLE_NAME.plugh", dst = "DST_TABLE_NAME.xyzzy", conv = "datetime" },
    { src = "SOURCE_TABLE_NAME.thud", dst = "DST_TABLE_NAME.wibble", conv = "timestamp"} 
]
```

### `[config]`
Most of the settings should be self-explanatory but here are some notes:

1. `log_level` - valid options are `debug`, `info`, `warn`, `error`, `fatal`, `panic`
1. `checkpoint_interval` - is expecting Go-style duration strings (e.g. `1s`, `1m`, `1h`, etc.)
1. `checkpoint_field` - is the field used for creating the "fetch query" TBD

### `[source]`
Settings used for specifying how to talk to the source (Mongo) database.

### `[destination]`
Settings used for specifying how to talk to the destination (Postgres) database.

### `[mapping]`
1. At least one mapping must exist
1. You can use a wildcard to match a field (e.g. `SOURCE_TABLE_NAME.foo_*`) but
it **SHOULD** match only one field; if it matches multiple - `mongo-migrator`
will use the first field it finds. When in doubt, define the field explicitly.
1. Valid `conv` options are: `int`, `string`, `float`, `bool`, `date`, `datetime`, `timestamp`, `bson`, `base64`
1. Only the fields listed in the mapping will be migrated. If you want to migrate
all fields, without specifying each field, use wildcards.
1. By default, if a field is not found in the source document, the field will be
_skipped_ and the migration will NOT fail. If you want the migration to fail when
a field is not found, set `required = true` in the mapping entry.

## Output
The output produced by `mmmbop` includes the following information:

1. Overall progress %
1. Estimated completion time
1. Number of documents migrated
1. Number of documents skipped
1. Number of documents with a failed conversion
1. Number of read errors
1. Number of insert errors
1. Number of connection retries
1. Number of checkpoint writes

`mmmbop` produces colorized output by default. To disable this behavior, pass
`--no-color` arg.

## Performance
This tool will _try_ to speed things up for the migration but there are still a
few things you can do to improve performance:

1. **Indexes**
    * Make sure you have indexes on the fields you are migrating
1. **Read replicas**
    * Use a read replica for the source database
1. **Limit returned fields**
    * The less mappings you have, the faster the migration will go
1. **Sharding**
    * If you can, shard the source database to increase parallelism
1. **Temporary vertical scaling**
    * If you can, temporarily increase the resources for the source (and/or destination) database sever(s).
