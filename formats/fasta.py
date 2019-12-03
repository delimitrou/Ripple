import boto3
import iterator
import util
from iterator import Delimiter, DelimiterPosition, OffsetBounds, Options
from typing import Any, ClassVar, Optional


class Iterator(iterator.Iterator[None]):
  delimiter: Delimiter = Delimiter(item_token=">", offset_token=">", position=DelimiterPosition.start)
  options: ClassVar[Options] = Options(has_header = False)
  identifiers = None

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)
