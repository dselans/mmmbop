mmmbop
======
An ultra-fast and resilient tool for migrating BSON dumps -> PostgreSQL.

It is specifically designed to be able to deal with migrating exceptionally large
amount of docs which requires checkpointing, idempotency, and parallelism.

If you need to migrate a small amount of data - there are probably better tools
suited for that task. If you need to migrate 10TB's of data over the course of 
a few days, this is the tool for you.

## Install
1. Download the latest release from the [releases page](https://github.com/dselans/mmmbop/releases)
put the binary in your path, `chmod +x` it and you're good to go.

## Usage
```
Usage: mmmbop -h
    -h, --help
            Show this help message
    -v, --version
            Show the version of the program
    -c, --config cfg.toml (default: config.toml)
            Path to the TOML configuration file
    -d, --dry-run
            Run the migration without writing to the destination database
    -m, --migrate
            Run the migration
    -r, --report-interval [interval]
            Change the reporting interval (default 5s)
    -o, --report-output [file]
            Write report output to file
    -R, --disable-resume
            Disable resuming from a checkpoint (start from scratch every time)
    -C, --disable-color
            Disable color output
```



## Configuration
The configuration file is a TOML file that looks like this:

```toml
[config]
## Number of workers we will use for this migration (default: num cores)
# num_workers = 4

## Number of documents we will fetch from the source database at a time (default: 1000)
# batch_size = 1000

## Where we will write checkpointing info to (default: mongo-migrate-checkpoint.json)
# checkpoint_file = "mmmbop-checkpoint.json"

## How often we will dump checkpointing info to disk (default: 1s)
# checkpoint_interval = "1s"

## By default, mmmbop will perform dupe checks when resuming a migration.
## Set this to 'true' to disable dupe checking.
# disable_dupecheck = true

## By default, mmmbop performs migrations using checkpointing. Checkpointing
## involves generating an index of the source file at the start of the migration
## and periodically writing checkpoints to the checkpoint file.
## 
## The index is a list of offsets that mmmbop can use to quickly jump to a specific
## point in the source file (rather than re-reading the whole file when a migration
## is resumed).
## 
## Checkpointing is most useful when performing large migrations that are prone
## to interruptions but less so if you are performing a quick, one-off migration.
## 
## Set this to 'true' to disable checkpointing.
# disable_checkpoint = true

[source]
# Full path to the source file containing BSON documents
file = "source.gzip"

# Valid options are "gzip" or "plain"
file_type = "gzip"

# Valid options are 'json'
file_contents = "json"

[destination]
# Destination database type. Valid options are 'postgres', 'mysql'
type = "postgres"

# Full DSN for destination database server
dsn = "postgres://user:password@localhost:5432/dbname"

[mapping]
foo_mapping = [
    { src = "foo", dst = "DST_TABLE_NAME.bar", conv = "int", required = true},
    { src = "baz", dst = "DST_TABLE_NAME.qux", conv = "string" },
    { src = "quux", dst = "DST_TABLE_NAME.corge", conv = "float" },
    { src = "grault", dst = "DST_TABLE_NAME.garply", conv = "bool" },
    { src = "waldo", dst = "DST_TABLE_NAME.fred", conv = "date" },
    { src = "plugh", dst = "DST_TABLE_NAME.xyzzy", conv = "datetime" },
    { src = "thud", dst = "DST_TABLE_NAME.wibble", conv = "timestamp", dupecheck = true } 
]
```

### `[config]`

Most of the settings are documented in the config using comments. Here is some
additional info:

1. `log_level` - valid options are `debug`, `info`, `warn`, `error`, `fatal`, `panic`
1. `checkpoint_interval` - is expecting Go-style duration strings (e.g. `1s`, `1m`, `1h`, etc.)
1. `checkpoint_index` -  at the start of the first migration, `mmmbop` generates
an "index" that is used for speeding up offset lookups when resuming an interrupted
migration.

### `[mapping]`
1. At least one mapping must exist
1. If `file_contents` is set to `json`, the `src` field should be the JSON field
name using [gjson syntax](https://github.com/tidwall/gjson) (ie. `foo.bar.baz`, 
`foo.0.bar` etc.)
1. Valid `conv` options are: `int`, `string`, `float`, `bool`, `date`, `datetime`, `timestamp`, `bson`, `base64`
1. Only the fields listed in the mapping will be migrated
1. By default, if a field is not found in the source document, the field will be
_skipped_ and the migration will NOT fail. If you want the migration to fail when
a field is not found, set `required = true` in the mapping entry.
1. Fields set as 'dupecheck' will be used for determining if destination already
contains the entry. You can disable dupe checking altogether by specifying
`disable_dupecheck = true` in the config section.
    * NOTE: At least **ONE** field PER TABLE must be set with the `dupecheck` property

## Output
The output produced by `mmmbop` includes the following information:

```json
{
  "progress": {
     "percent_complete": 0.1,
     "migration_started_at": "2021-01-01T00:00:00Z",
     "elapsed_time": "1h5m",
     "estimated_duration": "1h"
  },
  "stats": {
     "qps_src": "1000",
     "qps_dst": "1000",
     "documents_total": 90000000,
     "documents_migrated": 200000000,
     "checkpoint_total": 23922,
     "checkpoint_sec_ago": 0
  },
  "errors": {
     "documents_skipped": 0,
     "documents_skipped_sec_ago": 0,
     "errors_conv": 0,
     "errors_conv_sec_ago": 0,
     "errors_src_read": 0,
     "errors_src_read_sec_ago": 0,
     "errors_dst_write": 0,
     "errors_dst_write_sec_ago": 0,
     "conn_src_retries": 0,
     "conn_src_retries_sec_ago": 0,
     "conn_dst_retries": 0,
     "conn_dst_retries_sec_ago": 0
  }
}
```

`mmmbop` produces colorized output by default. To disable this, pass `--no-color`.

By default, reporting occurs every `10s` - you can change this by passing 
`--report-interval [interval]` ([using standard Go durations](https://pkg.go.dev/time#ParseDuration)).

## Benchmarks

# `TODO: Add benchmarks`

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
