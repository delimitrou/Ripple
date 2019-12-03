import argparse
import boto3
import os
import random
import time
import util


def create_s3_key_name(key, execute=None):
  now = time.time()
  nonce = random.randint(1, 1000)
  _, ext = os.path.splitext(key)

  m = {
    "prefix": "0",
    "timestamp": now,
    "nonce": nonce,
    "num_bins": 1,
    "bin": 1,
    "file_id": 1,
    "suffix": "tide",
    "num_files": 1,
    "ext": ext[1:],  # Remove period
  }

  if execute:
    m["execute"] = execute
  return util.file_name(m)


# TODO: Storage class?
def upload(bucket_name, key, input_bucket_name=None, execute=None):
  s3 = boto3.resource("s3")
  s3_key = create_s3_key_name(key, execute)

  start = time.time()
  if input_bucket_name is not None:
    config = boto3.s3.transfer.TransferConfig(multipart_threshold=64*1024*1024, max_concurrency=10,
                                              multipart_chunksize=16*1024*1024, use_threads=False)
    copy_source = {
      "Bucket": input_bucket_name,
      "Key": key,
    }

    done = False
    print("Moving {0:s} to s3://{1:s}".format(key, bucket_name), flush=True)
    while not done:
      try:
        s3.Bucket(bucket_name).copy(copy_source, s3_key, Config=config)
        done = True
      except Exception as e:
        print("ERROR: upload_input", e)
  else:
    print("Uploading {0:s} to s3://{1:s}".format(key, bucket_name), flush=True)
    config = boto3.s3.transfer.TransferConfig(multipart_threshold=64*1024*1024, max_concurrency=10,
                                              multipart_chunksize=16*1024*1024, use_threads=False)
    boto3.client("s3").upload_file(key, bucket_name, s3_key, Config=config)

  end = time.time()

  obj = s3.Object(bucket_name, s3_key)
  timestamp = obj.last_modified.timestamp()
  duration = end - start
  print("Uploaded key {0:s} as {1:s}. Upload Duration {2:f}. Last modified {3:f}.".format(key, s3_key, duration, timestamp), flush=True)

  return s3_key, timestamp, duration


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--destination_bucket_name", type=str, required=True, help="Destination bucket for object")
  parser.add_argument("--key", type=str, required=True, help="Name of object to upload")
  parser.add_argument("--source_bucket_name", type=str, default=None,
                      help="Source bucket for object. If not specified, script looks for object in data folder")
  args = parser.parse_args()
  upload(args.destination_bucket_name, args.key, args.source_bucket_name)


if __name__ == "__main__":
  main()
