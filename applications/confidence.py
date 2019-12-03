import boto3
import os
import subprocess
import util


def run(file, params, input_format, output_format, offsets):
  util.print_read(input_format, file, params)

  s3 = boto3.resource('s3')
  database_bucket = s3.Bucket(params["database_bucket"])

  with open("/tmp/crux", "wb") as f:
    database_bucket.download_fileobj("crux", f)

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  output_dir = "/tmp/confidence-crux-output-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])

  arguments = [
    "--output-dir", output_dir,
  ]

  command = "cd /tmp; ./crux assign-confidence {0:s} {1:s}".format(file, " ".join(arguments))
  subprocess.check_output(command, shell=True)

  output_files = []
  input_file = "{0:s}/assign-confidence.target.txt".format(output_dir)
  output_format["ext"] = "confidence"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  os.rename(input_file, output_file)
  output_files.append(output_file)

  return output_files
