import boto3
import pivot
import threading
import util
from database import Database
from typing import Any, Dict, List, Optional


def split_file(database: Database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  split_size = params["split_size"]

  input_bucket = bucket_name
  if util.is_set(params, "ranges"):
    if "input_prefix" in params:
      obj = database.get_entries(bucket_name, params["input_prefix"])[0]
      input_key = obj.key
      pivot_key = database.get_entries(bucket_name, "4/")[0].key # TODO Unhardcode
      [_, _, ranges] = pivot.get_pivot_ranges(bucket_name, pivot_key, params)
    else:
      [input_bucket, input_key, ranges] = pivot.get_pivot_ranges(bucket_name, key, params)
      obj = database.get_entry(input_bucket, input_key)
  else:
    input_key = key
    obj = database.get_entry(input_bucket, input_key)

  output_format["ext"] = obj.key.split(".")[-1]
  assert("ext" not in output_format or output_format["ext"] != "pivot")
  file_id = 1 
  content_length: int = obj.content_length()
#  num_files = int((content_length + split_size - 1) / split_size)
  num_files = 10

  threads = []
  token = "{0:f}-{1:d}".format(output_format["timestamp"], output_format["nonce"])
  while file_id <= num_files:
    offsets = [(file_id - 1) * split_size, min(content_length, (file_id) * split_size) - 1]
    extra_params = {**output_format, **{
      "file_id": file_id,
      "num_files": num_files,
      "offsets": offsets,
    }}

    if util.is_set(params, "ranges"):
      extra_params["pivots"] = ranges

    payload = database.create_payload(params["bucket"], input_key, extra_params)
    payload["log"] = [token, output_format["prefix"], output_format["bin"], output_format["num_bins"], file_id, num_files]

    threads.append(threading.Thread(target=database.invoke, args=(params["output_function"], payload)))
    threads[-1].start()
    file_id += 1

  for thread in threads:
    thread.join()
  return True


def handler(event, context):
  util.handle(event, context, split_file)
