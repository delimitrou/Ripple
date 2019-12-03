import inspect
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import TestEntry
from typing import Any, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import tsv


class TestIterator(tsv.Iterator):
  def __init__(self, entry: TestEntry, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    tsv.Iterator.__init__(self, entry, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_next(self):
    entry = TestEntry("test.tsv", "A\tB\tC\na\tb\tc\n1\t2\t3\n")

    # Requires multiple passes
    it = TestIterator(entry, None, 11, 11)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"A\tB\tC", b"a\tb\tc"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 11))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"1\t2\t3"])
    self.assertEqual(offset_bounds, OffsetBounds(12, 17))
    self.assertFalse(more)

    # Read everything in one pass
    it = TestIterator(entry, None, 20, 20)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"A\tB\tC", b"a\tb\tc", b"1\t2\t3"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 17))
    self.assertFalse(more)

  def test_combine(self):
    entry1 = TestEntry("test1.tsv", "HEADER\nA\tB\tC\na\tb\tc\n1\t2\t3\n")
    entry2 = TestEntry("test2.tsv", "HEADER\nD\tE\tF\nd\te\tf\n4\t5\t6\n")
    entry3 = TestEntry("test3.tsv", "HEADER\nG\tH\tI\ng\th\ti\n7\t8\t9\n")
    entry4 = TestEntry("test4.tsv", "HEADER\nJ\tK\tL\nj\tk\tl\n10\t11\t12\n")
    entries = [entry1, entry2, entry3, entry4]

    temp_name = "/tmp/ripple_test"
    with open(temp_name, "wb+") as f:
      tsv.Iterator.combine(entries, f, {})

    with open(temp_name) as f:
      self.assertEqual(f.read(), "HEADER\nA\tB\tC\na\tb\tc\n1\t2\t3\nD\tE\tF\nd\te\tf\n4\t5\t6\nG\tH\tI\ng\th\ti\n7\t8\t9\nJ\tK\tL\nj\tk\tl\n10\t11\t12\n")
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()
