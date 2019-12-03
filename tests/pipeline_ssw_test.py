import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict


class SmithWaterman(unittest.TestCase):
  def test_basic(self):
    pp: pipeline.Pipeline = pipeline.Pipeline("ssw/smith-waterman.json")

    pp.populate_table("ssw-database", "ssw/", ["uniprot.fasta"])
    pp.populate_table("ssw-program", "ssw/", ["ssw_test"])

    name = "0/123.400000-13/1-1/1-0.000000-1-fasta.fasta"
    pp.run(name, "ssw/input-10.fasta")

    entries: List[TestEntry] = pp.database.get_entries(pp.table.name)
    entry: TestEntry = entries[-1]
    actual_output: List[bytes] = filter(lambda item: len(item.strip()) > 0, entry.get_content().split(b"\n\n"))
    blast = pp.__import_format__("blast")
    actual_output = sorted(actual_output, key=lambda item: [blast.Iterator.get_identifier_value(item, blast.Identifiers.score), item])

    with open(pp.dir_path + "/ssw/output", "rb") as f:
      expected_output: List[bytes] = list(filter(lambda item: len(item.strip()) > 0, f.read().split(b"\n\n")))
    expected_output = sorted(expected_output, key=lambda item: [blast.Iterator.get_identifier_value(item, blast.Identifiers.score), item])

    self.assertCountEqual(actual_output, expected_output)
    pp.database.destroy()


if __name__ == "__main__":
    unittest.main()
