import pytest

import toolsql


column_expressions = [
    ('col1', 'col1'),
    ('COUNT(*)', 'COUNT(*)'),
    ('COUNT(DISTINCT col1)', 'COUNT(DISTINCT col1)'),
    ('MAX(col1)', 'MAX(col1)'),
    ('pi()', 'pi()'),
    ({'column': 'col1', 'cast': 'INTEGER'}, 'CAST(col1 AS INTEGER)'),
    ({'column': 'col1', 'alias': 'col1_alias'}, 'col1 AS col1_alias'),
    (
        {'column': 'col1', 'cast': 'INTEGER', 'alias': 'col1_alias'},
        'CAST(col1 AS INTEGER) AS col1_alias',
    ),
    (
        {'column': 'col1', 'encode': 'raw_hex'},
        {
            'postgresql': "encode(col1::bytea, 'hex') AS col1",
            'sqlite': 'lower(hex(col1)) AS col1',
        },
    ),
    (
        {'column': 'col1', 'encode': 'prefix_hex'},
        {
            'postgresql': "'0x' || encode(col1::bytea, 'hex') AS col1",
            'sqlite': "'0x' || lower(hex(col1)) AS col1",
        },
    ),
    (
        {'column': 'col1', 'encode': 'prefix_hex', 'alias': 'col1_alias'},
        {
            'postgresql': "'0x' || encode(col1::bytea, 'hex') AS col1_alias",
            'sqlite': "'0x' || lower(hex(col1)) AS col1_alias",
        },
    ),
]

invalid_column_expressions = [
    'col1;col2',
    'col1,col2',
]


_column_expression_to_str = toolsql.statements.statement_utils._column_expression_to_str


@pytest.mark.parametrize('test', column_expressions)
def test_build_column_expression(test):
    given_input, target_output = test
    if isinstance(target_output, str):
        actual_output = _column_expression_to_str(given_input, dialect='sqlite')
        assert actual_output == target_output
        actual_output = _column_expression_to_str(given_input, dialect='postgresql')
        assert actual_output == target_output
    elif isinstance(target_output, dict):
        for dialect, subtarget_output in target_output.items():
            actual_output = _column_expression_to_str(given_input, dialect=dialect)
            assert actual_output == subtarget_output
    else:
        raise Exception('unknown test type')


@pytest.mark.parametrize('test', invalid_column_expressions)
def test_invalid_column_expression(test):
    with pytest.raises(Exception):
        _column_expression_to_str(test)

