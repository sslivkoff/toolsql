# toolsql

toolsql makes it easy to build sql queries and execute those queries

toolsql's goals:
- provide identical interfaces to sqlite + postgresql
- provide nearly identical interfaces for synchronous + `async` usage
- minimize startup import time
- maximize read / write performance
- tight integration with connectorx and polars
- make it easy to drop into raw SQL when needed
- use basic python datatypes instead of custom objects
- mypy typing with `strict=True`

## Contents
- [Installation](#installation)
- [Example Usage](#example-usage)
- [Reference](#reference)

## Installation
- `pip install toolsql`
- requires python 3.7 - 3.11


## Example Usage

1. [Specify Database Configuration](#specify-database-configuration)
2. [Specify Table Schema](#specify-table-schema)
3. [DDL Statements](#ddl-statements)
4. [DML Statements](#dml-statements)
5. [Transactions](#transactions)
6. [`async` Functionality](#async-functionality)

#### Specify Database Configuration

```python
# sqlite
db_config = {
    'dbms': 'sqlite',
    'path': '/path/to/sqlite/file.db',
}

# postgresql
db_config = {
    'dbms': 'postgresql',
    'database': '<database_name>',
    'hostname': '<hostname>',
    'username': '<username>',
    'password': '<password>',
}
```

#### Specify Table Schema

```python
table = {
    'name': 'weather',
    'columns': [
        {'name': 'year', 'type': 'Integer', 'primary': True},
        {'name': 'month', 'type': 'Integer', 'primary': True},
        {'name': 'rainfall', 'type': 'Float'},
        {'name': 'temperature', 'type': 'Float'},
        {'name': 'country', 'type': 'Float'},
    ],
}
```

#### DDL Statements

```python

with toolsql.connect(db_config) as conn:

    # CREATE
    toolsql.create_table(
        table=table,
        conn=conn,
        confirm=True,
    )
    
    # DROP
    toolsql.drop_table(
        table=table,
        conn=conn,
        confirm=True,
    )
```

#### DML Statements

```python

rows = [
    (2020, 1, 8.2, 90, 'Turkey'),
    (2020, 5, 1.1, 50, 'Germany'),
    (2020, 9, 7.4, 60, 'UK'),
    (2021, 1, 5.2, 72, 'France'),
    (2021, 5, 2.1, 56, 'Argentina'),
    (2021, 9, 6.4, 68, 'Sweden'),
    (2022, 1, 1.2, 70, 'Indonesia'),
    (2022, 5, 4.1, 56, 'Vietnam'),
    (2022, 9, 9.4, 60, 'India'),
]

with toolsql.connect(db_config) as conn:

    # INSERT
    toolsql.insert(
        rows=rows,
        table=table,
        conn=conn,
    )
    
    # SELECT
    toolsql.select(
        where_equals={'country': 'Turkey'},
        table=table,
        conn=conn,
    )
    
    # UPDATE
    toolsql.update(
        values={'country': 'Türkiye'},
        where_equals={'country': 'Turkey'},
        table=table,
        conn=conn,
    )
    
    # DELETE
    toolsql.delete(
        where_equals={'country': 'Türkiye'},
        table=table,
        conn=conn,
    )
```

#### Transactions

```python
with toolsql.connect(db_config) as conn:
    with toolsql.transaction(conn)
        # will commit if context exits error-free
        # ...
```

#### `async` Functionality
```python
async with toolsql.async_connect(db_config) as conn:

    await toolsql.async_select(
        with_equals={'country': 'Turkey'},
        table=table,
        conn=conn,
    )

    async with toolsql.async_transaction():

        await toolsql.async_insert(
            rows=rows,
            table=table,
            conn=conn,
        )
        await toolsql.async_update(
            values={'country': 'Türkiye'},
            where_equals={'country': 'Turkey'},
            table=table,
            conn=conn,
        )
        await toolsql.async_delete(
            where_equals={'country': 'Türkiye'},
            table=table,
            conn=conn,
        )
```

## Reference

### Supported Executors
- `sqlite3`: sqlite sync reads
- `aiosqlite`: sqlite async reads
- `psycopg`: postgres sync / async reads
- `connectorx`: simple read queries

### `SELECT` input arguments
- `distinct`
- `where_equals`
- `where_gt`
- `where_gte`
- `where_lt`
- `where_lte`
- `where_like`
- `where_ilike`
- `where_in`
- `where_or`
- `order_by`
- `limit`
- `offset` 

### `SELECT` output formats
- `'tuple'`: each row is a tuple
- `'dict'`: each row is a dict
- `'cursor'`: query cursor
- `'polars'`: polars dataframe of rows
- `'pandas'`: pandas dataframe of rows
- `'single_tuple'`: single row of output as a tuple
- `'single_tuple_or_none'`: single row of output as a tuple
- `'single_dict'`: single row of output as a dict
- `'single_dict_or_none'`: single row of output as a dict
- `'cell'`: single column of single row
- `'cell_or_none'`: single column of single row
- `'single_column'`: single column
