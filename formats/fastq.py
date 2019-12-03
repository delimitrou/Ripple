import iterator
import re
import util
from enum import Enum
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Optional, Options
from typing import Any, ClassVar


class Identifiers(Enum):
  signature = 0


class Iterator(iterator.Iterator[Identifiers]):
  delimiter: Delimiter = Delimiter(item_token="\n", offset_token="\n", position=DelimiterPosition.inbetween, regex=b"(@([^\n]*\n){3}[^\n]*)")
  options: ClassVar[Options] = Options(has_header = False)
  identifiers: Identifiers
  signature_length: ClassVar[int] = 8
  base_ids = {"A": 0, "C": 1, "G": 2, "N": 3, "T": 4}

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def get_identifier_value(cls: Any, item: bytes, identifier: Identifiers) -> float:
    lines = item.decode("utf-8").strip().split("\n")
    if len(lines) != 4:
      print(lines)
    assert(len(lines) == 4)
    seq = lines[1]
    identifier = seq[0:7]

    for i in range(len(seq)-cls.signature_length+1):
      window = seq[i:i+7]
      if window < identifier:
        identifier = window

    identifier_value = 0
    for i in range(cls.signature_length):
      identifier_value *= len(cls.base_ids)
      identifier_value += cls.base_ids[seq[i]]

    assert(identifier_value >= 0)
    return identifier_value
