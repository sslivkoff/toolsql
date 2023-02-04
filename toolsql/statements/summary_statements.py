from __future__ import annotations

from toolsql import spec

from . import statement_utils


def build_get_table_nbytes_statement(
    table: str | spec.TableSchema,
    *,
    dialect: spec.Dialect,
) -> str:

    table_name = statement_utils.get_table_name(table)

    if dialect == 'sqlite':
        return """
        SELECT SUM("pgsize") as size
        FROM "dbstat"
        WHERE name = '{table_name}'
        GROUP BY name
        """.format(table_name=table_name)

    elif dialect == 'postgresql':
        return "SELECT * FROM pg_total_relation_size('{table_name}')".format(
            table_name=table_name
        )

    else:
        raise Exception('unknown dialect: ' + str(dialect))


def build_get_tables_nbytes_statement(
    *,
    dialect: spec.Dialect,
) -> str:

    if dialect == 'sqlite':
        # https://stackoverflow.com/a/58251635
        return """
        SELECT name as table_name, SUM("pgsize") as size
        FROM "dbstat"
        GROUP BY name
        """

    elif dialect == 'postgresql':
        # https://stackoverflow.com/a/2596678
        return """
        SELECT
            table_name::TEXT,
            (pg_total_relation_size('"' || table_schema || '"."' || table_name || '"')) AS size
        FROM information_schema.tables
            WHERE table_schema = 'public'
        ORDER BY
            pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') DESC
        """

    else:
        raise Exception('unknown dialect: ' + str(dialect))

