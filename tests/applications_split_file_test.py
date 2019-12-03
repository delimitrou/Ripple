import inspect
import json
import os
import sys
import unittest
import tutils
from tutils import TestDatabase

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import util
sys.path.insert(0, parentdir + "/lambda")
import split_file
sys.path.insert(0, parentdir + "/formats")


def get_invoke(name, bucket_name, key, prefix, offsets, file_id, num_files):
  payload = tutils.create_payload(bucket_name, key, prefix, file_id, num_files, offsets)

  return {
    "name": name,
    "type": "Event",
    "payload": json.JSONEncoder().encode(payload),
  }


class SplitFunction(unittest.TestCase):
  def check_payload_equality(self, expected_invokes, actual_invokes):
    self.assertEqual(len(expected_invokes), len(actual_invokes))
    for i in range(len(expected_invokes)):
      expected_invokes[i]["payload"] = json.JSONDecoder().decode(expected_invokes[i]["payload"])
      self.assertDictEqual(expected_invokes[i]["payload"], actual_invokes[i])

  def test_basic(self):
    database = TestDatabase()
    table1 = database.create_table("table1")
    log = database.create_table("log")
    entry1 = table1.add_entry("0/123.400000-13/1-1/1-1-0-suffix.new_line", "A B C\nD E F\nG H I\nJ K L\nM N O\nP Q R\n")
    input_format = util.parse_file_name(entry1.key)
    output_format = dict(input_format)
    output_format["prefix"] = 1

    params = {
      "execute": 0,
      "file": "split_file",
      "format": "new_line",
      "log": log.name,
      "name": "split",
      "output_function": "an-output-function",
      "ranges": False,
      "split_size": 20,
      "timeout": 60,
    }

    event = tutils.create_event(database, table1.name, entry1.key, params)
    context = tutils.create_context(params)
    split_file.handler(event, context)

    invoke1 = get_invoke("an-output-function", table1.name, entry1.key, prefix=1, offsets=[0, 19], file_id=1, num_files=2)
    invoke2 = get_invoke("an-output-function", table1.name, entry1.key, prefix=1, offsets=[20, 35], file_id=2, num_files=2)

    expected_invokes = [invoke1, invoke2]
    self.check_payload_equality(expected_invokes, database.payloads)


if __name__ == "__main__":
  unittest.main()
