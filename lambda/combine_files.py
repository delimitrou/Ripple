import importlib
import os
import util
from database import Database, Entry
from typing import Any, Dict, List


def combine(database: Database, table_name, key, input_format, output_format, offsets, params):
  output_format["file_id"] = input_format["bin"]
  output_format["bin"] = 1
  output_format["num_bins"] = 1
  output_format["num_files"] = input_format["num_bins"]
  file_name = util.file_name(output_format)
  util.make_folder(output_format)
  [combine, last_file, keys] = util.combine_instance(table_name, key, params)
  if combine:
    msg = "Combining TIMESTAMP {0:f} NONCE {1:d} BIN {2:d} FILE {3:d}"
    msg = msg.format(input_format["timestamp"], input_format["nonce"], input_format["bin"], input_format["file_id"])
    print(msg)

    format_lib = importlib.import_module(params["output_format"])
    iterator_class = getattr(format_lib, "Iterator")
    temp_name = "/tmp/{0:s}".format(file_name)
    # Make this deterministic and combine in the same order
    keys.sort()
    entries: List[Entry] = list(map(lambda key: database.get_entry(table_name, key), keys))
    metadata: Dict[str, str] = {}
    if database.contains(table_name, file_name):
      return True

    with open(temp_name, "wb+") as f:
      metadata = iterator_class.combine(entries, f, params)

    found = database.contains(table_name, file_name)
    if not found:
      with open(temp_name, "rb") as f:
        database.put(params["bucket"], file_name, f, metadata)
    os.remove(temp_name)
    return True
  else:
    return database.contains(table_name, file_name) or key != last_file


def handler(event, context):
  util.handle(event, context, combine)
