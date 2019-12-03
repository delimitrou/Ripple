import shutil
import subprocess
import util
import os
from database import Database
from typing import List


def download(database, bucket, path, name):
   with open(path + name, "wb+") as f:
     database.get_entry(bucket, name).download(f)
   subprocess.call("chmod 755 " + path + name, shell=True)


def run(database: Database, file: str, params, input_format, output_format, offsets: List[int]):
  print("compress", file)
  dir_path = "/tmp/fastore_test/"
  if not os.path.exists(dir_path):
    os.makedirs(dir_path)
  bin_path = dir_path #+ "bin/"
  if not os.path.exists(bin_path):
    os.makedirs(bin_path)
  script_path = dir_path #+ "script/"
  if not os.path.exists(script_path):
    os.makedirs(script_path)

  script_files = ["fastore_compress.sh"]
  bin_files = ["fastore_bin", "fastore_pack", "fastore_rebin"]

  for s in script_files:
    download(database, params["program_bucket"], script_path, s)

  for b in bin_files:
    download(database, params["program_bucket"], bin_path, b)

  with open(script_path + "reference.fq", "wb+") as f:
    database.get_entries(params["bucket"], params["input_prefix"])[0].download(f)

  input_file = file
  output_format["ext"] = "compress"

  arguments = [
      "in " + input_file,
      "pair reference.fq",
      "threads 2",
  ]

  command = "cd {0:s}; ./fastore_compress.sh --lossless --{1:s}".format(script_path, " --".join(arguments))
  print(command)
  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  print("after")

  output_files = ["OUT.cdata", "OUT.cmeta"]
  output_list = []
  for f in output_files:
    output_format["ext"] = f.split(".")[-1]
    output_file = "/tmp/{0:s}".format(util.file_name(output_format))
    shutil.move(script_path + f, output_file)
    output_list.append(output_file)

  return output_list
