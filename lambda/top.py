import heapq
import importlib
import util
from database import Database
from typing import Any, Dict, List


class Element:
  def __init__(self, identifier, value):
    self.value = value
    self.identifier = identifier

  def __lt__(self, other):
    return [self.identifier, self.value] < [other.identifier, other.value]


def find_top(d: Database, table: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  entry = d.get_entry(table, key)
  format_lib = importlib.import_module(params["format"])
  iterator = getattr(format_lib, "Iterator")
  it = iterator(entry, offsets)

  top = []
  more = True
  while more:
    [items, _, more] = it.next()

    for item in items:
      score: float = it.get_identifier_value(item, params["identifier"])
      heapq.heappush(top, Element(score, item))
      if len(top) > params["number"]:
        heapq.heappop(top)

  file_name = util.file_name(output_format)
  temp_name = "/tmp/{0:s}".format(file_name)
  items = list(map(lambda t: t.value, top))
  with open(temp_name, "wb+") as f:
    [content, metadata] = iterator.from_array(items, f, it.get_extra())

  with open(temp_name, "rb") as f:
    d.put(table, file_name, f, metadata)


def handler(event, context):
  util.handle(event, context, find_top)
