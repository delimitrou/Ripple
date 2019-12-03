import subprocess
import util
from database import Database
from typing import List


def run(database: Database, target_key: str, params, input_format, output_format, offsets: List[int]):
  with open("/tmp/ssw_test", "wb+") as f:
    database.download(params["program_bucket"], "ssw_test", f)

  subprocess.call("chmod 755 /tmp/ssw_test", shell=True)

  query_obj = database.get_entries(params["bucket"], params["input_prefix"])[0]
  query_key = query_obj.key
  with open("/tmp/{0:s}".format(query_key), "wb+") as f:
    query_obj.download(f)

  output_format["ext"] = "blast"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))

  command = "cd /tmp; ./ssw_test -p {0:s} {1:s} > {2:s}".format(target_key, query_key, output_file)

  subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
  return [output_file]
