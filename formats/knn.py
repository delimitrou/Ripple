import heapq
import iterator
import new_line
import util
from database import Entry
from iterator import OffsetBounds, Optional, Options
from typing import Any, BinaryIO, ClassVar, Dict, Iterable, List, Tuple


# KNN ITERATOR
# Currently, just supports RGA like values.
# Stores distance and classification of k nearest neighbors for each point.
# Expects files to put of the format
# x1 y1,d11 c11,d12 c12...d1k c1k
# x2 y2,d21 c21,d22 c22...d2k c2k
#         *
#         *
#         *
# xn yn,dn1 cn1,dn2 cn2...dnk cnk

Neighbors = Tuple[bytes, List[Tuple[float, int]]]


def __neighbor_from_bytes__(item: bytes) -> Tuple[float, int]:
  [distance, classification] = item.split(b' ')
  return (float(distance), int(classification))


def __neighbor_to_bytes__(n: Tuple[float, int]) -> bytes:
  return str.encode("{0:f} {1:d}".format(n[0], n[1]))


def __neighbors_from_bytes__(line: bytes) -> Neighbors:
  items = line.split(b',')
  neighbors: List[Tuple[float, int]] = list(map(lambda item: __neighbor_from_bytes__(item), items[1:]))
  assert(len(items[0].strip()) > 0)
  return (items[0], neighbors)


def __neighbors_to_bytes__(n: Neighbors) -> bytes:
  (point, neighbors) = n
  assert(len(point.strip()) > 0)
  neighbors = list(map(lambda i: __neighbor_to_bytes__(i), neighbors))
  return point + b',' + b','.join(neighbors)


class Iterator(new_line.Iterator):
  identifiers = None

  def __init__(self, obj: Entry, offset_bounds: Optional[OffsetBounds] = None):
    iterator.Iterator.__init__(self, Iterator, obj, offset_bounds)

  @classmethod
  def from_array(cls: Any, items: List[Neighbors], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    content = cls.delimiter.item_token.join(list(map(lambda n: __neighbors_to_bytes__(n), items)))

    if f:
      f.write(content)
    return (content, {})

  @classmethod
  def get_identifier_value(cls: Any, item: str, identifier: None) -> float:
    # I don't think an identifier makes sense for knn. However, this may change
    # if we ever come up with a reason to sort the values.
    [x, y] = item.split(",")[0].split(" ")
    return float(x + "." + y)

  @classmethod
  def to_array(cls: Any, content: bytes) -> Iterable[Neighbors]:
    items = filter(lambda item: len(item.strip()) > 0, content.split(cls.delimiter.item_token))
    return map(lambda n: __neighbors_from_bytes__(n), items)

  @classmethod
  def combine(cls: Any, entries: List[Entry], f: BinaryIO, extra: Dict[str, Any]) -> Dict[str, str]:
    if not util.is_set(extra, "sort"):
      return new_line.Iterator.combine(entries, f, extra)

    top_scores = {}
    for entry in entries:
      items = cls.to_array(entry.get_content())
      for item in items:
        [s, neighbors] = item
        if s not in top_scores:
          top_scores[s] = list(map(lambda n: (-1 * n[0], n[1]), neighbors))
          heapq.heapify(top_scores[s])
        else:
          for neighbor in neighbors:
            [score, classification] = neighbor
            if len(top_scores[s]) < extra["k"] or -1*score > top_scores[s][0][0]:
              heapq.heappush(top_scores[s], (-1*score, classification))
            if len(top_scores[s]) > extra["k"]:
              heapq.heappop(top_scores[s])

    keys = list(top_scores.keys())
    for i in range(len(keys)):
      if i > 0:
        f.write(b'\n')
      s = keys[i]
      line = s
      for [score, c] in top_scores[s]:
        line += str.encode(",{0:f} {1:d}".format(-1 * score, c))
      f.write(line)
    return {}
