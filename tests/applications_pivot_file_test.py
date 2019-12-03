import inspect
import os
import sys
import tutils
import unittest
from tutils import TestDatabase, TestEntry, TestTable

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/lambda")
import pivot_file
sys.path.insert(0, parentdir + "/formats")
import blast


class PivotMethods(unittest.TestCase):
  def test_basic(self):
    database: TestDatabase = TestDatabase()
    log: TestTable = database.create_table("log")
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/1-1-1-suffix.blast",
"""target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 2 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 290 suboptimal_alignment_score: 321

target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48""")

    params = {
      "bucket": table1.name,
      "execute": 0,
      "file": "sort",
      "format": "blast",
      "identifier": "score",
      "log": log.name,
      "name": "sort",
      "num_bins": 2,
      "database": database,
      "storage_class": "STANDARD",
      "timeout": 60,
    }

    event = tutils.create_event(database, table1.name, entry1.key, params)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    entries = database.get_entries(table1.name)
    self.assertEqual(len(entries), 2)
    self.assertEqual(entries[1].get_content().decode("utf-8"), "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format(table1.name, entry1.key))

  def test_offsets(self):
    database: TestDatabase = TestDatabase()
    log: TestTable = database.create_table("log")
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("0/123.400000-13/1-1/1-1-1-suffix.blast",
"""target_name: 1
query_name: 1
optimal_alignment_score: 540 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 2 suboptimal_alignment_score: 9

target_name: 1
query_name: 1
optimal_alignment_score: 300 suboptimal_alignment_score: 112

target_name: 1
query_name: 1
optimal_alignment_score: 290 suboptimal_alignment_score: 321

target_name: 1
query_name: 1
optimal_alignment_score: 193 suboptimal_alignment_score: 48""")

    params = {
      "bucket": table1.name,
      "execute": 0,
      "file": "sort",
      "format": "blast",
      "identifier": "score",
      "log": log.name,
      "name": "sort",
      "num_bins": 2,
      "database": database,
      "storage_class": "STANDARD",
      "timeout": 60,
    }

    event = tutils.create_event(database, table1.name, entry1.key, params)
    context = tutils.create_context(params)
    pivot_file.handler(event, context)
    entries = database.get_entries(table1.name)
    self.assertEqual(len(entries), 2)
    self.assertEqual(entries[1].get_content().decode("utf-8"), "{0:s}\n{1:s}\n2009.0\t290321.0\t540010.0".format(table1.name, entry1.key))


if __name__ == "__main__":
  unittest.main()
