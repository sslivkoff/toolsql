from __future__ import annotations


def statement_to_single_line(sql: str) -> str:
    import re

    # https://stackoverflow.com/a/1546245
    return re.sub('[\n\t ]{2,}', ' ', sql).strip()

