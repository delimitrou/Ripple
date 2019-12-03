import new_line
from iterator import OffsetBounds, Options
from typing import Any, ClassVar, Generic, List, Optional, TypeVar


T = TypeVar("T")


class Iterator(Generic[T], new_line.Iterator[T]):
  identifiers: T
  item_delimiter: ClassVar[str] = "\t"
  options: ClassVar[Options] = Options(has_header = True)

  def __init__(self, obj: Any, offset_bounds: Optional[OffsetBounds] = None):
    new_line.Iterator.__init__(self, obj, offset_bounds)

  @classmethod
  def to_tsv_array(cls: Any, items: List[str]) -> List[List[str]]:
    tsv_items: List[List[str]] = list(map(lambda item: item.split(cls.item_delimiter), items))
    return tsv_items

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: T) -> float:
    raise Exception("Not Implemented")
