import os
import shutil
import subprocess
import util
from database import Database
from typing import List


def run(database: Database, file: str, params, input_format, output_format, offsets: List[int]):
  if "species" in params:
    species = params["species"]
  else:
    raise Exception("Tide needs species parameter specified")

  database.download(params["database_bucket"], "{0:s}/fasta".format(species), "/tmp/fasta")
  database.download(params["database_bucket"], "crux", "/tmp/crux")

  subprocess.call("chmod 755 /tmp/crux", shell=True)
  index_files = ["auxlocs", "pepix", "protix"]
  if not os.path.isdir("/tmp/fasta.index"):
    os.mkdir("/tmp/fasta.index")

  for index_file in index_files:
    name = "{0:s}/{1:s}".format(species, index_file)
    database.download(params["database_bucket"], name, "/tmp/fasta.index/{0:s}".format(index_file))

  output_dir = "/tmp/crux-output-{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])

  arguments = [
    "--num-threads", str(params["num_threads"]),
    "--txt-output", "T",
    "--concat", "T",
    "--output-dir", output_dir,
    "--overwrite", "T",
  ]

  command = "cd /tmp; ./crux tide-search {0:s} fasta.index {1:s}".format(file, " ".join(arguments))
  try:
    subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  except subprocess.CalledProcessError as exc:
    print("Status : FAIL", exc.returncode, exc.output)
    raise exc
  input_file = "{0:s}/tide-search.txt".format(output_dir)
  output_format["suffix"] = species
  output_format["ext"] = "txt"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  os.rename(input_file, output_file)
  shutil.rmtree(output_dir)

  return [output_file]
