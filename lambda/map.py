import pivot
import util
from database import Database, Entry
from typing import Any, Dict, List


def map_file(database: Database, table: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  prefix: str = util.key_prefix(key)

  if util.is_set(params, "ranges"):
    [bucket_name, key, ranges] = pivot.get_pivot_ranges(table, key)
    items: List[Entry] = database.get_entries(table, prefix)
    keys: List[str] = list(set(map(lambda item: item.key, items)))
  else:
    if "map_bucket_key_prefix" in params:
      items: List[Entry] = database.get_entries(params["map_bucket"], prefix=params["map_bucket_key_prefix"])
      keys: List[str] = list(set(map(lambda item: item.key, items)))
    else:
      items: List[Entry] = database.get_entries(params["map_bucket"])
      if params["directories"]:
        items = list(filter(lambda item: "/" in item.key, items))
        keys: List[str] = list(set(map(lambda item: item.key.split("/")[0], items)))
      else:
        keys: List[str] = list(set(map(lambda item: item.key, items)))

  file_id = 0
  num_files = len(keys)
  keys.sort()
  for i in range(num_files):
    target_file = keys[i]
    file_id += 1

    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": table,
          },
          "object": {
          },
          "extra_params": {
            "target_bucket": params["map_bucket"],
            "target_file": target_file,
            "prefix": output_format["prefix"],
            "file_id": file_id,
            "num_files": num_files,
          }
        }
      }]
    }

    if params["input_key_value"] == "key":
      payload["Records"][0]["s3"]["object"]["key"] = key
      payload["Records"][0]["s3"]["extra_params"][params["bucket_key_value"]] = target_file
    elif params["bucket_key_value"] == "key":
      payload["Records"][0]["s3"]["object"]["key"] = target_file
      payload["Records"][0]["s3"]["extra_params"][params["input_key_value"]] = key
    else:
      raise Exception("Need to specify field for map key")

    if util.is_set(params, "ranges"):
      payload["Records"][0]["s3"]["extra_params"]["pivots"] = ranges
      payload["Records"][0]["s3"]["pivots"] = ranges

    database.invoke(params["output_function"], payload)


def handler(event, context):
  util.handle(event, context, map_file)
