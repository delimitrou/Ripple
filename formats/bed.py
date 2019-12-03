import new_line
from enum import Enum
from iterator import OffsetBounds, Options
from typing import Any, ClassVar, Generic, List, Optional, TypeVar


T = TypeVar("T")


class Identifiers(Enum):
  start_position = 0
  end_position = 1


class Iterator(new_line.Iterator):
  identifiers: Identifiers

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    new_line.Iterator.__init__(self, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: bytes, identifier: T) -> float:
    parts = item.split(b"\t")
    return float(parts[identifier.value + 1])

