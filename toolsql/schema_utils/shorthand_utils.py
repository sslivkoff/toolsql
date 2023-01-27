from __future__ import annotations

from toolsql import spec


def normalize_shorthand_table_schema(
    table: spec.TableSchema | spec.TableSchemaShorthand,
) -> spec.TableSchema:

    if isinstance(table['columns'], (list, tuple)):
        columns = table['columns']
    elif isinstance(table['columns'], dict):
        columns = []
        for name, column in table['columns'].items():
            if isinstance(column, (str, type)):
                columns.append({'name': name, 'type': column})
            elif isinstance(column, dict):
                if 'name' in column:
                    if column['name'] != name:
                        raise Exception(
                            'conflicting name in table specification'
                        )
                    columns.append(column)
                else:
                    columns.append(dict(column, name=name))
            else:
                raise Exception('unknown column format: ' + str(type(column)))
    else:
        raise Exception(
            'unknown columns format: ' + str(type(table['columns']))
        )

    constraints = table.get('constraints')
    if constraints is None:
        constraints = []

    indices = table.get('indices')
    if indices is None:
        indices = []

    return {
        'name': table['name'],
        'columns': columns,
        'constraints': constraints,
        'indices': indices,
    }

