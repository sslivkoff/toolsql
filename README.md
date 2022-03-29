# toolsql

toolsql makes it simple to work with db schemas and queries

the goal is to create an opinionated lightweight wrapper around SqlAlchemy that lowers boilerlpace and cognitive overhead without resorting to magic


## Features
- define schemas, queries, and migrations using JSON
- compatible with postgresql and sqlite
- uses sqlalchemy under the hood

## Contents
1. Installation
2. Usage
3. Reference

## Installation

## Usage

#### Log In To Database


#### Inspect Current Database Schema


#### Define CLI

#### Insert, Select, Update, and Delete Rows

## Reference
1. Module Reference
2. CLI Reference
3. Schema Reference
4. Function Reference

#### Module Reference
- `toolsql.cli` defines command line interface for toolsql
- `toolsql.crud_utils` functions for inserting, selecting, updating, and deleting rows
- `toolsql.migrate_utils` functions for migrations
- `toolsql.sqlalchemy` functions for interfacing with sqlalchemy
- `toolsql.admin_utils`
- `toolsql.summary_utils`

#### CLI Reference

#### Schema Reference
- `SQLDBSchema` describes a database, including its tables
- `SQLTableSpec` describes a table, including its columns, indices, and constraints
- `SQLColumn` describes a column, including its datatype and constraints
- see `spec.py` for specific structure of each schema

