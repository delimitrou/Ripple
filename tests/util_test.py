import inspect
import os
import sys
import unittest
import tutils
from unittest.mock import MagicMock
from tutils import TestDatabase

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util


class FileNameMethods(unittest.TestCase):
  def test_file_name_parser(self):
    m = {
      "prefix": 0,
      "timestamp": 123.4,
      "nonce": 42,
      "bin": 12,
      "num_bins": 13,
      "file_id": 3,
      "execute": False,
      "num_files": 4,
      "suffix": "hello",
      "ext": "txt"
    }
    self.assertDictEqual(m, util.parse_file_name(util.file_name(m)))
    self.assertEqual("0/123.400000-13/1-4/1-0.000000-0-suffix.txt", util.file_name(util.parse_file_name("0/123.400000-13/1-4/1-0.000000-0-suffix.txt")))


class ExecutionMethods(unittest.TestCase):
  def test_run(self):
    params = {
      "file": "application",
      "log": "log",
      "name": "util",
      "test": True,
      "timeout": 60,
    }

    s3 = TestDatabase()
    bucket1 = s3.create_table("bucket1")
    log = s3.create_table("log")
    bucket1.add_entry("0/123.400000-13/1-1/1-1-0-suffix.txt", "")
    bucket1.add_entry("1/123.400000-13/1-1/1-1-0-suffix.txt", "")
    bucket1.add_entry("0/123.400000-13/1-1/2-1-1-suffix.txt", "")
    bucket1.add_entry("1/123.400000-13/1-1/2-1-1-suffix.txt", "")
    log.add_entry("0/123.400000-13/1-1/3-1-1-suffix.log", "")
    log.add_entry("1/123.400000-13/1-1/3-1-1-suffix.log", "")

    event = tutils.create_event(s3, bucket1.name, "0/123.4-13/1-1/1-1-0-suffix.txt", params)
    context = tutils.create_context(params)

    # Call on entry that doesn't have a log entry
#    func = MagicMock()
#    util.handle(event, context, func)
#    self.assertTrue(func.called)

    # Call on entry that does have a log entry
    func = MagicMock()
    event["Records"][0]["s3"]["bucket"]["name"] = bucket1.name
    event["Records"][0]["s3"]["object"]["key"] = "0/123.400000-13/1-1/3-1-1-suffix.txt"
    util.handle(event, context, func)
    self.assertFalse(func.called)


if __name__ == "__main__":
  unittest.main()
