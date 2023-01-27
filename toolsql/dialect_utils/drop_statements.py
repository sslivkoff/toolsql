
def build_drop_statement(
    table_name: str,
    *,
    if_exists: bool = True,
) -> str:
    """
    - sqlite: https://www.sqlite.org/lang_droptable.html
    - postgresql: https://www.postgresql.org/docs/current/sql-droptable.html
    """
    return """DROP TABLE {if_exists}{table_name}""".format(
        table_name=table_name,
        if_exists='IF EXISTS ' if if_exists else '',
    )

