# from __future__ import annotations


# def create_select_statement_cx(
#     table: str,
#     row_id=row_id,
#     row_ids=row_ids,
#     where_equals=where_equals,
#     where_like=where_like,
#     where_ilike=where_ilike,
#     where_in=where_in,
#     where_foreign_row_equals=where_foreign_row_equals,
#     where_gt=where_gt,
#     where_gte=where_gte,
#     where_lt=where_lt,
#     where_lte=where_lte,
#     where_start_of=where_start_of,
#     filters=filters,
#     filter_by=filter_by,
#     columns=only_columns,
#     sql_functions=sql_functions,
#     order_by=order_by,
# ) -> str:

#     # specify columns
#     if isinstance(columns, str):
#         columns = columns
#     elif isinstance(columns, list):
#         columns = ','.join(columns)
#     elif isinstance(columns, dict):
#         column_pairs = [
#             column_name + ' as ' + column_alias
#             for column_name, column_alias in columns.items()
#         ]
#         columns = ','.join(column_pairs)
#     elif columns is None:
#         columns = '*'
#     else:
#         raise Exception('unknown columns format')

#     # construct initial statement
#     statement = 'SELECT {columns} FROM {table}'
#     statement = statement.format(columns=columns, table=table)

#     # order by
#     if order_by is not None:
#         if isinstance(order_by, str):
#             order_by = order_by
#         elif isinstance(order_by, list):
#             order_by = ','.join(_order_by_to_str(order_by))
#         elif isinstance(order_by, dict):
#             order_by = _order_by_to_str(order_by)
#         else:
#             raise Exception('unknown order_by format')

#         statement = statement + ' ' + order_by

#     return statement


# class OrderByDict(TypedDict):
#     column: str
#     asc: bool
#     desc: bool


# def _order_by_to_str(order_by: str | OrderByDict):

#     if isistance(order_by, str):
#         return order_by

#     elif isinstance(order_by, dict):

#         # determine direction
#         asc = order_by.get('asc')
#         desc = order_by.get('desc')
#         if asc is not None and desc is not None:
#             if asc == desc:
#                 raise Exception('conflicting asc and desc specifications')
#         elif desc is not None:
#             asc = not desc
#         else:
#             asc = True

#         # create answer
#         column = order_by['column']
#         if asc:
#             return column + ' ASC'
#         else:
#             return column + ' DESC'

#     else:
#         raise Exception('')

