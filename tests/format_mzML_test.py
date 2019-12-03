import inspect
import os
import sys
import unittest
import xml.etree.ElementTree as ET
from iterator import OffsetBounds
from tutils import TestDatabase, TestEntry, TestTable

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir + "/formats")
import mzML

INPUT = """<?xml version="1.0" encoding="utf-8"?>
<indexedmzML>
  <mzML>
    <run id="run_id">
      <spectrumList count="3">
        <spectrum id="controllerType=0 controllerNumber=1 scan=1" index="0">
          <cvParam name="ms level" value="2"/>
          <cvParam name="base peak m/z" value="123"/>
        </spectrum>
        <spectrum id="controllerType=0 controllerNumber=1 scan=2" index="1">
          <cvParam name="ms level" value="2"/>
          <cvParam name="base peak m/z" value="4"/>
        </spectrum>
        <spectrum id="controllerType=0 controllerNumber=1 scan=3" index="2">
          <cvParam name="ms level" value="2"/>
          <cvParam name="base peak m/z" value="566"/>
          <cvParam />
        </spectrum>
      </spectrumList>
    </run>
  </mzML>
  <indexList count="1">
    <index name="spectrum">
      <offset idRef="controllerType=0 controllerNumber=1 scan=1">123</offset>
      <offset idRef="controllerType=0 controllerNumber=1 scan=2">321</offset>
      <offset idRef="controllerType=0 controllerNumber=1 scan=3">517</offset>
    </index>
  </indexList>
  <indexListOffset>774</indexListOffset>
</indexedmzML>
"""

CHROMATOGRAM_INPUT = """<?xml version="1.0" encoding="utf-8"?>
<indexedmzML>
  <mzML>
    <run id="run_id">
      <spectrumList count="3">
        <spectrum id="controllerType=0 controllerNumber=1 scan=1" index="0">
          <cvParam name="ms level" value="2"/>
        </spectrum>
        <spectrum id="controllerType=0 controllerNumber=1 scan=2" index="1">
          <cvParam name="ms level" value="2"/>
          <cvParam />
        </spectrum>
        <spectrum id="controllerType=0 controllerNumber=1 scan=3" index="2">
          <cvParam name="ms level" value="2"/>
          <cvParam />
          <cvParam />
        </spectrum>
      </spectrumList>
      <chromotogramList count="1" defaultDataProcessingRef="pwiz_Reader_Thermo_conversion">
        <chromatogram index="0" id="TIC" defaultArrayLength=24426">
          <cvParam />
        </chromatogram>
      </chromotogramList>
    </run>
  </mzML>
  <indexList count="2">
    <index name="spectrum">
      <offset idRef="controllerType=0 controllerNumber=1 scan=1">123</offset>
      <offset idRef="controllerType=0 controllerNumber=1 scan=2">267</offset>
      <offset idRef="controllerType=0 controllerNumber=1 scan=3">433</offset>
    </index>
    <index name="chromatogram">
      <offset idRef="TIC">641</offset>
    <index>
  </indexList>
  <indexListOffset>890</indexListOffset>
</indexedmzML>
"""

class IteratorMethods(unittest.TestCase):
  def test_metadata(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("0/123.4-13/1/1-1-1-test.mzML", INPUT)
    it = mzML.Iterator(entry1)
    self.assertEqual(it.header_start_index, 0)
    self.assertEqual(it.header_end_index, 122)
    self.assertEqual(it.spectra_start_index, 123)
    self.assertEqual(it.spectra_end_index, 734)
    self.assertEqual(it.chromatogram_start_index, -1)
    self.assertEqual(it.chromatogram_end_index, -1)
    self.assertEqual(it.index_list_offset, 774)
    self.assertEqual(it.footer_start_index, 735)
    self.assertEqual(it.footer_end_index, len(entry1.get_content()))

    entry2: TestEntry = table1.add_entry("0/123.4-13/1/1-1-1-ctest.mzML", CHROMATOGRAM_INPUT)
    it = mzML.Iterator(entry2)
    self.assertEqual(it.header_start_index, 0)
    self.assertEqual(it.header_end_index, 122)
    self.assertEqual(it.spectra_start_index, 123)
    self.assertEqual(it.spectra_end_index, 618)
    self.assertEqual(it.chromatogram_start_index, 641)
    self.assertEqual(it.chromatogram_end_index, 847)
    self.assertEqual(it.index_list_offset, 890)
    self.assertEqual(it.footer_start_index, 619)
    self.assertEqual(it.footer_end_index, len(entry2.get_content()))

  def test_next(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("0/123.4-13/1/1-1-1-test.mzML", INPUT)
    it = mzML.Iterator(entry1)
    [spectra, offset_bounds, more] = it.next()
    spectra = list(spectra)
    self.assertFalse(more)
    self.assertEqual(len(spectra), 3)
    self.assertEqual(spectra[0].get("id"), "controllerType=0 controllerNumber=1 scan=1")
    self.assertEqual(spectra[1].get("id"), "controllerType=0 controllerNumber=1 scan=2")
    self.assertEqual(spectra[2].get("id"), "controllerType=0 controllerNumber=1 scan=3")

  def test_identifier(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("0/123.4-13/1/1-1-1-test.mzML", INPUT)
    it = mzML.Iterator(entry1)
    [items, offset_bounds, more] = it.next()
    spectra = list(items)
    self.assertEqual(it.get_identifier_value(spectra[0], mzML.Identifiers.mass), 123.0)
    self.assertEqual(it.get_identifier_value(spectra[1], mzML.Identifiers.mass), 4.0)
    self.assertEqual(it.get_identifier_value(spectra[2], mzML.Identifiers.mass), 566.0)

  def test_adjust(self):
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    entry1: TestEntry = table1.add_entry("0/123.4-13/1/1-1-1-test.mzML", INPUT)

    # Multiple spectra start in range
    it = mzML.Iterator(entry1, OffsetBounds(120, 540))
    [spectra, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(offset_bounds.start_index, 123)
    self.assertEqual(offset_bounds.end_index, 733)
    self.assertEqual(len(list(spectra)), 3)

    # One spectra starts in range
    it = mzML.Iterator(entry1, OffsetBounds(120, 250))
    [spectra, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(offset_bounds.start_index, 123)
    self.assertEqual(offset_bounds.end_index, 320)

    # No spectra start in range
    it = mzML.Iterator(entry1, OffsetBounds(126, 240))
    [spectra, offset_bounds, more] = it.next()
    self.assertFalse(more)
    self.assertEqual(offset_bounds.start_index, 123)
    self.assertEqual(offset_bounds.end_index, 320)

  def test_combine(self):
    metadata = {
      "header_start_index": "0",
      "header_end_index": "122",
      "spectra_start_index": "123",
      "spectra_end_index": "619",
      "chromatogram_start_index": "641",
      "chromatogram_end_index": "847",
      "index_list_offset": "890",
      "footer_start_index": "619",
      "footer_end_index": "1340",
    }
    database: TestDatabase = TestDatabase()
    table1: TestTable = database.create_table("table1")
    table1.add_entry("0/123.4-13/1/1-1-2-test.mzML", CHROMATOGRAM_INPUT)
    table1.add_entry("0/123.4-13/1/2-1-2-test.mzML", CHROMATOGRAM_INPUT)

    params = {
      "chunk_size": 100,
      "s3": database,
    }
    temp_file = "/tmp/test_combine"
    metadata: Dict[str, str] = {}
    with open(temp_file, "wb+") as f:
      metadata = mzML.Iterator.combine(database.get_entries(table1.name), f, {})
    self.assertEqual(metadata["count"], "6")
    self.assertEqual(metadata["header_start_index"], "0")
    self.assertEqual(metadata["header_end_index"], "122")
    self.assertEqual(metadata["spectra_start_index"], "123")
    self.assertEqual(metadata["spectra_end_index"], "1121")
    self.assertEqual(metadata["chromatogram_start_index"], "-1")
    self.assertEqual(metadata["chromatogram_end_index"], "-1")
    self.assertEqual(metadata["index_list_offset"], "1150")
    self.assertEqual(metadata["footer_start_index"], "1136")
    self.assertEqual(metadata["footer_end_index"], "2020")


if __name__ == "__main__":
  unittest.main()
