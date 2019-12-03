import inspect
import numpy as np
import os
import sys
import unittest
from iterator import OffsetBounds
from tutils import TestDatabase, TestEntry
from typing import Any, Optional

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")

import classification


class TestIterator(classification.Iterator):
  def __init__(self, entry: TestEntry, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    classification.Iterator.__init__(self, entry, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_array(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")

    zeros = np.zeros([3, 3, 3], dtype=int)
    content: bytes = zeros.tostring() + str.encode(" 1\r\n")
    nonzeros = np.copy(zeros)
    nonzeros[0][0] = [255, 255, 255]
    nonzeros[1][1] = [1, 2, 3]
    content += nonzeros.tostring() + str.encode(" 0")
    entry1: TestEntry = table1.add_entry("test.classification", content)

    items = list(classification.Iterator.to_array(entry1.get_content()))
    expected = [(zeros.flatten(), 1), (nonzeros.flatten(), 0)]
    self.assertEqual(len(items), 2)
    self.assertEqual(tuple(items[0][0]), tuple(expected[0][0]))
    self.assertEqual(items[0][1], expected[0][1])
    self.assertEqual(tuple(items[1][0]), tuple(expected[1][0]))
    self.assertEqual(items[1][1], expected[1][1])

    [actual_content, metadata] = classification.Iterator.from_array(items, None, {})
    self.assertSequenceEqual(actual_content, content)

  def test_next(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")

    zeros = np.zeros([3, 3, 3], dtype=int)
    content: bytes = zeros.tostring() + str.encode(" 1\r\n")
    nonzeros = np.copy(zeros)
    nonzeros[0][0] = [255, 255, 255]
    nonzeros[1][1] = [1, 2, 3]
    content += nonzeros.tostring() + str.encode(" 0")
    entry1: TestEntry = table1.add_entry("test.classification", content)

    it: TestIterator = TestIterator(entry1, None, 300, 300)
    [items, bounds, more] = it.next()
    self.assertTrue(more)
    self.assertEqual(len(list(items)), 1)

    [items, bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(len(list(items)), 1)


if __name__ == "__main__":
  unittest.main()
