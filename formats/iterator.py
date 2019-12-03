import boto3
import heapq
import os
import re
import util
from database import Entry
from enum import Enum
from typing import Any, BinaryIO, ClassVar, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar


T = TypeVar("T")


class DelimiterPosition(Enum):
  start = 1
  inbetween = 2
  end = 3


class Delimiter:
  def __init__(self, item_token: str, offset_token: str, position: DelimiterPosition, regex=None):
    self.item_token = str.encode(item_token)
    self.offset_token = str.encode(offset_token)
    if regex:
      self.regex = re.compile(regex, re.MULTILINE)
    else:
      self.regex = None
    if position == DelimiterPosition.start:
      self.item_regex = re.compile(b"^" + self.item_token, re.MULTILINE)
      self.offset_regex = re.compile(b"^" + self.offset_token, re.MULTILINE)
    else:
      self.item_regex = re.compile(self.item_token)
      self.offset_regex = re.compile(self.offset_token)
    self.position = position


class OffsetBounds:
  def __init__(self, start_index: int, end_index: int):
    assert(start_index < end_index)
    self.start_index = start_index
    self.end_index = end_index

  def __eq__(self, other):
    return self.start_index == other.start_index and self.end_index == other.end_index

  def __repr__(self):
    return "[{0:d},{1:d}]".format(self.start_index, self.end_index)


class Options:
  def __init__(self, has_header: bool):
    self.has_header = has_header


class Iterator(Generic[T]):
  adjust_chunk_size: ClassVar[int] = 1000
  next_index: int = -1
  options: ClassVar[Options]
  read_chunk_size: ClassVar[int] = 10*1000*1000
  delimiter: Delimiter
  identifiers: T

  def __init__(self, cls: Any, entry: Entry, offset_bounds: Optional[OffsetBounds]):
    self.cls = cls
    self.item_count = None
    self.entry = entry
    self.offset_bounds = offset_bounds
    self.offsets: List[int] = []
    self.remainder: bytes = b''
    self.__setup__()

  def __adjust__(self, end_index: int, token) -> int:
    content: bytes = self.entry.get_range(max(end_index - self.adjust_chunk_size, 0), end_index)
    last_byte: int = len(content) - 1
    index = list(token.finditer(content))[-1].span()[0] + len(self.delimiter.offset_token) - 1
    offset_index: int = last_byte - index
    assert(offset_index >= 0)
    return offset_index

  def __setup__(self):
    if self.offset_bounds:
      self.start_index = self.offset_bounds.start_index
      self.end_index = min(self.offset_bounds.end_index, self.entry.content_length() - 1)
      if self.start_index != 0:
        self.start_index -= self.__adjust__(self.start_index, self.delimiter.offset_regex)
        if self.delimiter.position != DelimiterPosition.start:
          # Don't include delimiter
          self.start_index += 1
      if self.end_index != (self.entry.content_length() - 1):
        self.end_index -= self.__adjust__(self.end_index, self.delimiter.offset_regex)
        if self.delimiter.position == DelimiterPosition.start:
          self.end_index -= 1
    else:
      self.start_index = 0
      self.end_index = self.entry.content_length() - 1

    assert(self.start_index <= self.end_index)
    self.content_length = self.end_index - self.start_index
    self.offsets = [self.next_index]

  @classmethod
  def combine(cls: Any, entries: List[Entry], f: BinaryIO, extra: Dict[str, Any]) -> Dict[str, str]:
    metadata: Dict[str, str] = {}

    if util.is_set(extra, "sort"):
      items = []
      for i in range(len(entries)):
        it = cls(entries[i], None)
        sub_items = it.get(it.get_start_index(), it.get_end_index())
        items += list(map(lambda item: (it.get_identifier_value(item, extra["identifier"]), item), sub_items))
      items = sorted(items, key=lambda i: i[0])
      items = list(map(lambda i: i[1], items))
      content, metadata = cls.from_array(items, f, extra)
      f.write(content)
    else:
      count = 0
      for i in range(len(entries)):
        entry = entries[i]
        if entry.content_length() == 0:
          continue
        if count > 0 and cls.delimiter.position == DelimiterPosition.inbetween:
          f.seek(-1 * len(cls.delimiter.item_token), os.SEEK_END)
          end: str = f.read(len(cls.delimiter.item_token))
          if end != cls.delimiter.item_token:
            f.write(cls.delimiter.item_token)
        if cls.options.has_header and count > 0:
          lines = entry.get_content().split(cls.delimiter.item_token)[1:]
          content = cls.delimiter.item_token.join(lines)
          f.write(content)
        else:
          # TODO: There seems to be a bug where if I do entry.download(f), it's not guaranteed
          # the entire file will write at the end. I need to figure out why because downloading,
          # loading into memory and then writing to disk is slower.
          f.write(entry.get_content())
        count += 1

    return metadata

  @classmethod
  def from_array(cls: Any, items: List[Any], f: Optional[BinaryIO], extra: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    metadata: Dict[str, str] = {}
    if cls.delimiter.position == DelimiterPosition.inbetween:
      content = cls.delimiter.item_token.join(items)
    else:
      content = b"".join(items)

    if f:
      f.write(content)
    return (content, metadata)

  @classmethod
  def to_array(cls: Any, content: bytes) -> Iterable[Any]:
    token = cls.delimiter.item_regex
    if cls.delimiter.regex is not None:
      items = map(lambda item: item[0], re.findall(cls.delimiter.regex, content))
    else:
      items: Iterable[bytes] = filter(lambda item: len(item.strip()) > 0, re.split(token, content))
      if cls.delimiter.position == DelimiterPosition.start:
        items = map(lambda item: cls.delimiter.item_token + item, items)
      elif cls.delimiter.position == DelimiterPosition.end:
        items = map(lambda item: item + cls.delimiter.item_token, items)
    return items

  @classmethod
  def get_identifier_value(cls: Any, item: bytes, identifier: T) -> float:
    raise Exception("Not Implemented")

  def get(self, start_byte: int, end_byte: int) -> Iterable[Any]:
    content: bytes = self.entry.get_range(start_byte, end_byte)
    return self.to_array(content)

  def get_extra(self) -> Dict[str, Any]:
    return {}

  def get_item_count(self) -> int:
    raise Exception("Not Implemented")

  def get_end_index(self) -> int:
    return self.end_index

  def get_start_index(self) -> int:
    return self.start_index

  def get_offset_end_index(self) -> int:
    return self.end_index

  def get_offset_start_index(self) -> int:
    return self.start_index

  def next(self) -> Tuple[Iterable[Any], Optional[OffsetBounds], bool]:
    if self.next_index == -1:
      self.next_index = self.get_offset_start_index()
    next_start_index: int = self.next_index
    next_end_index: int = min(next_start_index + self.read_chunk_size, self.get_offset_end_index())
    more: bool = True
    stream: bytes = self.entry.get_range(next_start_index, next_end_index)
    stream = self.remainder + stream
    if next_end_index == self.get_offset_end_index():
      next_start_index -= len(self.remainder)
      self.remainder = b''
      more = False
    else:
      token = self.delimiter.offset_token
      index: int = stream.rindex(token) if token in stream else -1
      if index != -1:
        if self.delimiter.position == DelimiterPosition.inbetween:
          index += len(self.delimiter.offset_token)
        next_end_index -= (len(stream) - index)
        next_start_index -= len(self.remainder)
        self.remainder = stream[index:]
        stream = stream[:index]
      else:
        self.remainder = stream
        next_end_index -= len(self.remainder)
        stream = b''
    self.next_index = min(next_end_index + len(self.remainder) + 1, self.get_offset_end_index())
    offset_bounds: Optional[OffsetBounds]
    if len(stream) == 0:
      offset_bounds = None
    else:
      offset_bounds = OffsetBounds(next_start_index, next_end_index)

    [stream, offset_bounds] = self.transform(stream, offset_bounds)
    return (self.to_array(stream), offset_bounds, more)

  def transform(self, stream: bytes, offset_bounds: Optional[OffsetBounds]) -> Tuple[bytes, Optional[OffsetBounds]]:
    return (stream, offset_bounds)
