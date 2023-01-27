# toolsql

## goals
- provider **minimal** wrapper around raw sql and around db-specific drivers
- provide sync and async interfaces
- support sqlite and postgresql
- minimize startup import time
- maximize read/write performance
- provide tight integration with polars


Two levels of abstraction:
1. raw sql
2. python wrapper around common sql operations

If python does not cover some functionality, make it easy to drop into SQL

## requirements
- python 3.7 - 3.11


## supported executors
- `sqlite3`: sqlite sync reads
- `aiosqlite`: sqlite async reads
- `psycopg`: postres sync / async reads
- `connectorx`: simple read queries


## `SELECT` output formats
- `'tuple'`: each row is a tuple
- `'dict'`: each row is a dict
- `'cursor'`: query cursor
- `'polars'`: polars dataframe of rows
- `'pandas'`: pandas dataframe of rows

