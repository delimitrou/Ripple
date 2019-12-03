import os
import shutil
import subprocess
import util
from database import Database
from typing import List


def run(database: Database, file: str, params, input_format, output_format, offsets: List[int]):
  database.download(params["database_bucket"], "crux", "/tmp/crux")
  subprocess.call("chmod 755 /tmp/crux", shell=True)
  output_dir = "/tmp/percolator-crux-output-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])

  arguments = [
    "--subset-max-train", str(params["max_train"]),
    "--quick-validation", "T",
    "--output-dir", output_dir,
  ]

  command = "cd /tmp; ./crux percolator {0:s} {1:s}".format(file, " ".join(arguments))
  try:
    subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  except subprocess.CalledProcessError as exc:
    print("Status : FAIL", exc.returncode, exc.output)
    raise exc

  output_files = []
  for item in ["target.{0:s}".format(params["output"])]:
    input_file = "{0:s}/percolator.{1:s}.txt".format(output_dir, item)
    output_format["ext"] = "percolator"
    output_file = "/tmp/{0:s}".format(util.file_name(output_format))
    os.rename(input_file, output_file)
    output_files.append(output_file)
  shutil.rmtree(output_dir)

  return output_files
