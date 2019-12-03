import argparse
import boto3


def clear(bucket_name, token=None, prefix=None):
  s3 = boto3.resource("s3")
  bucket = s3.Bucket(bucket_name)
  if prefix is None and token is None:
    bucket.objects.all().delete()
  elif prefix is None and token is not None:
    objects = list(bucket.objects.all())
    objects = list(filter(lambda o: token == o.key.split("/")[1], objects))
    for obj in objects:
      s3.Object(bucket_name, obj.key).delete()
  elif prefix is not None and token is None:
    bucket.objects.filter(Prefix=str(prefix)).delete()
  else:
    bucket.objects.filter(Prefix=str(prefix) + "/" + token).delete()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--bucket_name", type=str, required=True, help="Bucket to clear")
  parser.add_argument("--token", type=str, default=None, help="Only delete objects with the specified timestamp / nonce pair")
  parser.add_argument("--prefix", type=int, default=None, help="Only delete objects with the specified prefix")
  args = parser.parse_args()
  clear(args.bucket_name, args.token, args.prefix)


if __name__ == "__main__":
  main()
