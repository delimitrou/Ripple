import boto3
import iterator
import util
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Options
from typing import Any, ClassVar, Generic, Optional, TypeVar


T = TypeVar("T")


class Iterator(Generic[T], iterator.Iterator[T]):
  delimiter: Delimiter = Delimiter(item_token="\n", offset_token="\n", position=DelimiterPosition.inbetween)
  options: ClassVar[Options] = Options(has_header = False)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)
