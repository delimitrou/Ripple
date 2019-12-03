import pipeline
import unittest
from tutils import TestEntry, TestTable
from typing import Any, Dict


class Spacenet(unittest.TestCase):
  def test_basic(self):
    pp: pipeline.Pipeline = pipeline.Pipeline("spacenet/spacenet-classification.json")
    pp.populate_table("spacenet", "spacenet/", ["train.classification.w1-h1"])

    name = "0/123.400000-13/1-1/1-0.000000-1-image.tif"
    pp.run(name, "spacenet/3band_AOI_1_RIO_img1457.tif")



if __name__ == "__main__":
    unittest.main()
