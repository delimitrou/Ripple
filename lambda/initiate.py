import util
from database import Database
from typing import Any, Dict, List


def initiate(database: Database, bucket_name: str, key: str, input_format: Dict[str, Any], output_format: Dict[str, Any], offsets: List[int], params: Dict[str, Any]):
  [combine, keys, last] = util.combine_instance(bucket_name, key, params)
  if "trigger_key" in params:
    bucket = params["trigger_bucket"]
    key = params["trigger_key"]
  else:
    bucket = bucket_name
    key = database.get_entries(bucket_name, params["input_prefix"])[0].key 

  if combine:
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": bucket
          },
          "object": {
            "key": key
          },
          "extra_params": output_format
        }
      }]
    }
    database.invoke(params["output_function"], payload)


def handler(event, context):
  util.handle(event, context, initiate)
