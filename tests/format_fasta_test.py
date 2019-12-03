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
import fasta


class TestIterator(fasta.Iterator):
  def __init__(self, entry: TestEntry, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    fasta.Iterator.__init__(self, entry, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_next(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.fasta", ">A\tB\tC\n>a\tb\tc\n>1\t2\t3\n")

    # Read everything in one pass
    it = TestIterator(entry1, None, 30, 30)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b">A\tB\tC\n", b">a\tb\tc\n", b">1\t2\t3\n"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 20))
    self.assertFalse(more)

    # Requires multiple passes
    it = TestIterator(entry1, None, 8, 8)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b">A\tB\tC\n"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 6))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b">a\tb\tc\n"])
    self.assertEqual(offset_bounds, OffsetBounds(7, 13))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b">1\t2\t3\n"])
    self.assertEqual(offset_bounds, OffsetBounds(14, 20))
    self.assertFalse(more)

  def test_offsets(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.fasta", ">A\tB\tC\n>a\tb\tc\n>1\t2\t3\n")

    it = TestIterator(entry1, OffsetBounds(10, 15), 30, 30)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b">a\tb\tc\n"])
    self.assertEqual(offset_bounds, OffsetBounds(7, 13))

    # Edge case. Offset bound with end of file.
    it = TestIterator(entry1, OffsetBounds(14, 20), 30, 30)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b">1\t2\t3\n"])
    self.assertEqual(offset_bounds, OffsetBounds(14, 20))
    self.assertFalse(more)


if __name__ == "__main__":
  unittest.main()
