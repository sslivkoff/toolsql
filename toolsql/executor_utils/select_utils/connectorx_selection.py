from __future__ import annotations

from toolsql import conn_utils
from toolsql import spec
from . import row_formats


def _select_connectorx(
    *,
    sql: str | None = None,
    conn: str | spec.DBConfig,
    output_format: spec.QueryOutputFormat,
) -> spec.RowOutput:

    import connectorx  # type: ignore

    if output_format == 'pandas':
        result_format = 'pandas'
    else:
        result_format = 'polars'

    if not isinstance(conn, str):
        if isinstance(conn, dict):
            conn = conn_utils.get_db_uri(conn)
        else:
            raise Exception('unknown conn format: ' + str(type(conn)))

    result = connectorx.read_sql(conn, sql, return_type=result_format)
    return row_formats.format_row_dataframe(result, output_format=output_format)


async def _async_select_connectorx(
    *,
    sql: str | None = None,
    conn: str,
    output_format: spec.QueryOutputFormat,
) -> spec.RowOutput:

    # see https://github.com/sfu-db/connector-x/discussions/368
    # see https://stackoverflow.com/a/69165563
    import asyncio

    return await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: _select_connectorx(
            conn=conn,
            sql=sql,
            output_format=output_format,
        ),
    )
    # return await asyncio.to_thread(
    #     lambda: _select_connectorx(
    #         conn=conn,
    #         sql=sql,
    #         output_format=output_format,
    #     )
    # )

