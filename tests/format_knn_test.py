import inspect
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import TestDatabase, TestEntry
from typing import Any, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import knn


class TestIterator(knn.Iterator):
  def __init__(self, entry: TestEntry, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    knn.Iterator.__init__(self, entry, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_array(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.knn", "1.0 2.0 255 123 0,0.100000 0,0.300000 1,0.500000 1\n4.0 4.0 255 255 255,0.600000 2,0.660000 1,0.700000 0")

    items = list(knn.Iterator.to_array(entry1.get_content()))
    expected = [(b"1.0 2.0 255 123 0", [(0.1, 0), (0.3, 1), (0.5, 1)]), (b"4.0 4.0 255 255 255", [(0.6, 2), (0.66, 1), (0.7, 0)])]
    self.assertEqual(len(items), 2)
    self.assertEqual(items[0], expected[0])
    self.assertEqual(items[1], expected[1])

    [content, metadata] = knn.Iterator.from_array(items, None, {})
    self.assertEqual(entry1.get_content(), content)

  def test_combine(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test1.knn", "1.0 2.0 255 123 0,0.100000 0,0.300000 1,0.500000 1\n4.0 4.0 255 255 255,0.600000 2,0.660000 1,0.700000 0")
    entry2: TestEntry = table1.add_entry("test2.knn", "1.0 2.0 255 123 0,0.400000 0,0.600000 1,0.700000 1\n2.0 4.0 255 255 255,0.100000 2,0.160000 1,0.300000 0")
    temp_name = "/tmp/ripple_test"
    with open(temp_name, "wb+") as f:
      knn.Iterator.combine([entry1, entry2], f, {"k": 3, "sort": True})

    with open(temp_name) as f:
      content: str = f.read().strip()
    items = sorted(content.split("\n"))
    self.assertEqual(items, ["1.0 2.0 255 123 0,0.400000 0,0.300000 1,0.100000 0",
                             "2.0 4.0 255 255 255,0.300000 0,0.160000 1,0.100000 2",
                             "4.0 4.0 255 255 255,0.700000 0,0.660000 1,0.600000 2"
                             ])


if __name__ == "__main__":
  unittest.main()
