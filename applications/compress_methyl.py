import os
import subprocess
import util


def run(database, file, params, input_format, output_format, offsets):
  work_dir = "/tmp/{0:f}-{1:d}".format(input_format["timestamp"], input_format["nonce"])
  os.mkdir(work_dir)
  program_path = "{0:s}/output".format(work_dir)
  input_name = work_dir + "/input"
  os.rename(file, input_name)

  database.download(params["program_bucket"], "output", program_path)
  subprocess.call("chmod 755 " + program_path, shell=True)

  arguments = [
    program_path,
    "compress",
    input_name,
    work_dir
  ]

  command = " ".join(arguments)
  util.check_output(command)

  output_files = []
  result_dir = "{0:s}/compressed_input".format(work_dir)
  for subdir, dirs, files in os.walk(result_dir):
    for f in files:
      output_format["suffix"] = f.split("_")[-1].split("-")[0]

      output_file = "/tmp/{0:s}".format(util.file_name(output_format))
      os.rename("{0:s}/compressed_input/{1:s}".format(work_dir, f), output_file)
      output_files.append(output_file)

  return output_files
