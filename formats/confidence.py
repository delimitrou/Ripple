import tsv
import util
from enum import Enum
from iterator import OffsetBounds
from typing import Any, BinaryIO, ClassVar, Dict, List, Optional, Tuple


class Identifiers(Enum):
  qvalue = 9


class Iterator(tsv.Iterator[Identifiers]):
  threshold: ClassVar[float] = 0.01
  identifiers: Identifiers
  options: ClassVar[Options] = Options(has_header = True)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    tsv.Iterator.__init__(self, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: Identifiers) -> float:
    return float(cls.to_tsv_array(item)[identifier])

  def sum(self, identifier: Identifiers) -> int:
    [count, total] = self.fraction(identifier)
    return count

  def fraction(self, identifier: Identifiers) -> Tuple[int, int]:
    more: bool = True
    count: int = 0
    total: int = 0
    while more:
      total += 1
      [items, _, more] = self.next()
      for item in items:
        if float(self.get_identifier_value(item, Identifiers.qvalue)) <= self.threshold:
          count += 1
    return (count, total)
