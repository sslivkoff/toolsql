from __future__ import annotations

import typing

from . import sa_spec


TableRef = typing.Union[str, sa_spec.SATable]
Row = typing.Union[typing.Mapping[str, typing.Any], sa_spec.SARow]

