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
sys.path.insert(0, parentdir)
import util
sys.path.insert(0, parentdir + "/applications")
import draw_borders


class DrawBorder(unittest.TestCase):
  def test_basic(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    with open("spacenet/1-1-1-tide.knn", "rb") as f:
      entry1: TestEntry = table1.add_entry("5/1550529206.039528-957/1-1/1-1-1-tide.knn", f.read())
    with open("spacenet/3band_AOI_1_RIO_img147.tif", "rb") as f:
      entry2: TestEntry = table1.add_entry("0/1550529206.039528-957/1-1/1-1-1-tide.tiff", f.read())

    params = {
      "bucket": table1.name,
      "input_prefix": 0
    }
    input_format = util.parse_file_name(entry1.key)
    output_format = dict(input_format)
    output_format["prefix"] = 6
    util.make_folder(output_format)
    draw_borders.run(database, entry1.key, params, input_format, output_format, [])


if __name__ == "__main__":
  unittest.main()
