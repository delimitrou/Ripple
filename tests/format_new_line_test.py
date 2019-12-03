import inspect
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import TestDatabase, TestEntry, TestTable
from typing import Any, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import new_line


class TestIterator(new_line.Iterator):
  def __init__(self, entry: TestEntry, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    new_line.Iterator.__init__(self, entry, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_adjust(self):
    database: TestDatabase = TestDatabase()
    log: TestTable = database.create_table("log")
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.new_line", "A B C\na b c\n1 2 3\nD E F\nd e f\n")
    it = TestIterator(entry1, OffsetBounds(8, 13), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"a b c"])
    self.assertEqual(offset_bounds, OffsetBounds(6, 11))
    self.assertFalse(more)

    # No adjustment needed
    it = TestIterator(entry1, OffsetBounds(6, 11), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"a b c"])
    self.assertEqual(offset_bounds, OffsetBounds(6, 11))
    self.assertFalse(more)

    # Beginning of content
    it = TestIterator(entry1, OffsetBounds(0, 7), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(list(items), [b"A B C"])

    # Beginning of content
    it = TestIterator(entry1, OffsetBounds(26, entry1.content_length() - 1), 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(list(items), [b"d e f"])

  def test_next(self):
    database: TestDatabase = TestDatabase()
    log: TestTable = database.create_table("log")
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.new_line", "A B C\na b c\n1 2 3\n")

    # Requires multiple passes
    it = TestIterator(entry1, None, 11, 11)
    [items, offset_bounds, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(OffsetBounds(0, 11), offset_bounds)
    self.assertEqual(list(items), [b"A B C", b"a b c"])

    [items, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(list(items), [b"1 2 3"])

  def test_overflow(self):
    database: TestDatabase = TestDatabase()
    log: TestTable = database.create_table("log")
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.new_line", "A B C D E F G H\na b c d e f g h\n1 2 3 4 5 6 7 8 9\n")

    # Requires multiple passes
    it = TestIterator(entry1, None, 10, 10)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [])
    self.assertEqual(offset_bounds, None)
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"A B C D E F G H"])
    self.assertEqual(offset_bounds, OffsetBounds(0, 15))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"a b c d e f g h"])
    self.assertEqual(offset_bounds, OffsetBounds(16, 31))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [])
    self.assertEqual(offset_bounds, None)
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), [b"1 2 3 4 5 6 7 8 9"])
    self.assertEqual(offset_bounds, OffsetBounds(32, 49))
    self.assertFalse(more)

  def test_combine(self):
    database: TestDatabase = TestDatabase()
    log: TestTable = database.create_table("log")
    table1: TestTable = database.create_table("table1")
    table1.add_entry("test1.new_line", "A B C\na b c\n1 2 3\n")
    table1.add_entry("test2.new_line", "D E F\nd e f\n4 5 6\n")
    table1.add_entry("test3.new_line", "G H I\ng h i\n7 8 9\n")
    table1.add_entry("test4.new_line", "J K L\nj k l\n10 11 12\n")
    entries: List[TestEntry] = database.get_entries(table1.name)

    temp_name = "/tmp/ripple_test"
    with open(temp_name, "wb+") as f:
      new_line.Iterator.combine(entries, f, {})

    with open(temp_name) as f:
      self.assertEqual(f.read(), "".join(list(map(lambda entry: entry.get_content().decode("utf-8"), entries))))
    os.remove(temp_name)


if __name__ == "__main__":
  unittest.main()
