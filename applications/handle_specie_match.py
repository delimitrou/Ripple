import boto3
import json
import util


def run(key, params, input_format, output_format, offsets):
  objects = util.get_objects(params["bucket"], "0/", params)
  assert(len(objects) == 1)
  object_key = objects[0].key
  match = open(key).read()

  payload = {
    "Records": [{
      "s3": {
        "bucket": {
          "name": params["bucket"],
        },
        "object": {
          "key": object_key,
        },
        "extra_params": {
          "prefix": output_format["prefix"],
          "species": util.parse_file_name(match)["suffix"]
        }
      }
    }]
  }

  client = boto3.client("lambda")
  response = client.invoke(
    FunctionName=params["output_function"],
    InvocationType="Event",
    Payload=json.JSONEncoder().encode(payload)
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)

  return []
