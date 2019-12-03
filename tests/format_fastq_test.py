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
import fastq


expected_items = [
b"""@cluster_29:UMI_GCAGGA
CCCCCTTAAATAGCTGTTTATTTGGCCCCAG
+
8;;;>DC@DAC=B?C@9?B?CDCB@><<??A""",
b"""@cluster_39:UMI_GAACCG
CCTTCCATCACCAGATCGGAAAAACACACGC
+
00>7;8@5<192?/8;0;;>=3=/3239713""",
b"""@cluster_43:UMI_GGATTG
GAGTTATAATCCAATCTTTATTTAAAAATCT
+
>=AEC?C@;??0A>?0DEB9EEB@DDC1?=6"""
]


class TestIterator(fastq.Iterator):
  def __init__(self, entry: TestEntry, offset_bounds: Optional[OffsetBounds], adjust_chunk_size: int, read_chunk_size: int):
    self.adjust_chunk_size = adjust_chunk_size
    self.read_chunk_size = read_chunk_size
    fastq.Iterator.__init__(self, entry, offset_bounds)


class IteratorMethods(unittest.TestCase):
  def test_next(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")

    entry1: TestEntry = table1.add_entry("test.fastq", b"\n".join(expected_items))
    # Read everything in one pass
    it = TestIterator(entry1, None, 300, 300)
    [items, offset_bounds, more] = it.next()
    items = list(items)

    self.assertEqual(len(items), 3)
    self.assertEqual(items, expected_items)
    self.assertEqual(offset_bounds, OffsetBounds(0, 265))
    self.assertFalse(more)

    # Requires multiple passes
    it = TestIterator(entry1, None, 100, 100)
    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), expected_items[:1])
    self.assertEqual(offset_bounds, OffsetBounds(0, 88))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), expected_items[1:2])
    self.assertEqual(offset_bounds, OffsetBounds(89, 177))
    self.assertTrue(more)

    [items, offset_bounds, more] = it.next()
    self.assertEqual(list(items), expected_items[2:])
    self.assertEqual(offset_bounds, OffsetBounds(178, 265))
    self.assertFalse(more)

  def test_offsets(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("test.fastq", b"\n".join(expected_items))

    it = TestIterator(entry1, OffsetBounds(100, 200), 100, 100)
    [items, offset_bounds, more] = it.next()
    items = list(items)
    self.assertEqual(items, expected_items[1:2])
    self.assertEqual(offset_bounds, OffsetBounds(89, 177))
    self.assertFalse(more)

    # Edge case. Offset bound with end of file.
    it = TestIterator(entry1, OffsetBounds(200, 300), 200, 200)
    [items, offset_bounds, more] = it.next()
    items = list(items)
    self.assertEqual(items, expected_items[2:])
    self.assertEqual(offset_bounds, OffsetBounds(178, 265))
    self.assertFalse(more)


if __name__ == "__main__":
  unittest.main()
