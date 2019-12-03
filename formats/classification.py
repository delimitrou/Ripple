import iterator
import new_line
import numpy as np
import re
from database import Entry
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Options
from typing import Any, BinaryIO, ClassVar, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union


# CLASSIFICATION ITERATOR
# Currently, just supports RGA like values.
# Expects files to put of the format
# nparray(f11 f12 ... f1m) classification1
# nparray(f21 f22 ... f2m) classification2
#         *
#         *
#         *
# nparray(fn1 fn2 ... fnm) classification


Classification = Tuple[List[int], int]
T = TypeVar("T")


def __to_classification__(item: bytes) -> Classification:
  parts = item.split(b' ')
  classification = int(parts[-1])
  features = np.frombuffer(b' '.join(parts[:-1]), dtype=int)
  return (features, classification)


def __from_classification__(c: Classification) -> bytes:
  return c[0].tostring() + str.encode(" {c}".format(c=c[1]))


class Iterator(Generic[T], iterator.Iterator[T]):
  delimiter: Delimiter = Delimiter(item_token="\r\n", offset_token="\r\n", position=DelimiterPosition.inbetween)
  options: ClassVar[Options] = Options(has_header=False)

  def __init__(self, obj: Entry, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def from_array(cls: Any, items: List[Classification], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[Union[bytes, str], Dict[str, str]]:
    content: bytes = cls.delimiter.item_token.join(list(map(lambda item: __from_classification__(item), items)))
    if f:
      f.write(content)
    return (content, {})

  @classmethod
  def to_array(cls: Any, content: Union[bytes, str]) -> Iterable[Classification]:
    items = filter(lambda item: len(item.strip()) > 0, re.split(cls.delimiter.item_regex, content))
    return map(lambda item: __to_classification__(item), items)
