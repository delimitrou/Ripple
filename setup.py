import argparse
import boto3
import botocore
import json
import os
import shutil
import subprocess
import util


def get_layers(region, account_id, imports):
  layers = []
  for imp in imports:
    if imp  == "PIL":
        layers.append("arn:aws:lambda:{0:s}:{1:d}:layer:PIL:2".format(region, account_id))
    elif imp == "sklearn":
      layers.append("arn:aws:lambda:{0:s}:{1:d}:layer:Sklearn:1".format(region, account_id))
    elif imp == "numpy":
      layers.append("arn:aws:lambda:{0:s}:{1:d}:layer:numpy:1".format(region, account_id))
    else:
      raise Exception("Cannot find layer for", imp)
  return layers

def upload_function_code(client, zip_file, name, p, create):
  zipped_code = open(zip_file, "rb").read()
  fparams = p["functions"][name]
  account_id = int(boto3.client("sts").get_caller_identity().get("Account"))
  if create:
    response = client.create_function(
      FunctionName=name,
      Runtime="python3.6",
      Role="arn:aws:iam::{0:d}:role/{1:s}".format(account_id, p["role"]),
      Handler="{0:s}.handler".format(fparams["file"]),
      Code={
        "ZipFile": zipped_code
      },
      Timeout=p["timeout"],
      MemorySize=fparams["memory_size"],
      Layers=get_layers(p["region"], account_id, fparams["imports"])
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 201)

  else:
    response = client.update_function_code(
      FunctionName=name,
      ZipFile=zipped_code
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    response = client.update_function_configuration(
      FunctionName=name,
      Timeout=p["timeout"],
      MemorySize=fparams["memory_size"],
      Layers=get_layers(p["region"], account_id, fparams["imports"])
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    try:
      client.remove_permission(
        FunctionName=name,
        StatementId=name + "-" + p["bucket"]
      )
    except Exception as e:
      print("Cannot remove permissions for", name, p["bucket"])


def create_parameter_files(zip_directory, function_name, params):
  for i in range(len(params["pipeline"])):
    pparams = params["pipeline"][i]
    if pparams["name"] == function_name:
      p = {**params["functions"][function_name], **pparams}
      for value in ["timeout", "num_bins", "bucket", "storage_class", "log", "scheduler"]:
        if value in params:
          p[value] = params[value]

      name = "{0:d}.json".format(i)
      json_path = "{0:s}/{1:s}".format(zip_directory, name)
      f = open(json_path, "w")
      f.write(json.dumps(p))
      f.close()


def copy_file(directory, file_path):
  index = file_path.rfind("/")
  file_name = file_path[index + 1:]
  shutil.copyfile(file_path, "{0:s}/{1:s}".format(directory, file_name))
  return file_name


def zip_libraries(zip_directory, dependencies):
  shutil.copytree(dependencies, zip_directory)


def zip_formats(zip_directory, fparams, params):
  if "format" in fparams:
    form = fparams["format"]
    if "dependencies" in params and form in params["dependencies"]["formats"]:
      for file in params["dependencies"]["formats"][form]:
        copy_file(zip_directory, file)
    copy_file(zip_directory, "formats/{0:s}.py".format(form))


def zip_application(zip_directory, fparams):
  if "application" in fparams:
    copy_file(zip_directory, "applications/{0:s}.py".format(fparams["application"]))


def zip_ripple_file(zip_directory, fparams):
  file = "{0:s}.py".format(fparams["file"])
  shutil.copyfile("lambda/{0:s}".format(file), "{0:s}/{1:s}".format(zip_directory, file))
  for file in ["formats/iterator.py", "formats/pivot.py", "database.py", "util.py"]:
    copy_file(zip_directory, file)
  for format in fparams["formats"]:
    copy_file(zip_directory, "formats/{0:s}.py".format(format))



def upload_function(client, name, functions, params):
  zip_directory = "lambda_dependencies"
  zip_file = "lambda.zip"

  if os.path.isdir(zip_directory):
    shutil.rmtree(zip_directory)

  fparams = params["functions"][name]
  os.makedirs(zip_directory)

  zip_ripple_file(zip_directory, fparams)
  zip_application(zip_directory, fparams)
  zip_formats(zip_directory, fparams, params)
  create_parameter_files(zip_directory, name, params)
  os.chdir(zip_directory)
  subprocess.call("zip -r9 ../{0:s} .".format(zip_file), shell=True)
  os.chdir("..")
  upload_function_code(client, zip_file, name, params, name not in functions)
  os.remove(zip_file)
  shutil.rmtree(zip_directory)


def add_sort_pipeline(sort_params):
  functions = {}
  pipeline = []

  fformat = sort_params["format"]

  def add_function(name, function_params, pipeline_params):
    if name in functions:
      raise Exception("Cannot add sort pipeline. Function with name " + name + " already exists")

    function_params["memory_size"] = 1024
    functions[name] = function_params
    pipeline_params["name"] = name
    pipeline.append(pipeline_params)

  add_function("split-" + fformat, {
    "file": "split_file",
    "format": fformat,
    "split_size": sort_params["chunk_size"],
    "output_function": "pivot-" + fformat,
  }, {
    "ranges": False,
  })

  add_function("pivot-" + fformat, {
    "file": "pivot_file",
    "format": fformat,
    "identifier": sort_params["identifier"],
  }, {})

  add_function("combine-pivot-" + fformat, {
    "file": "combine_files",
    "format": "pivot",
    "sort": True,
  }, {})

  name = "split-" + fformat
  if name in functions:
    pipeline.append({
      "name": name,
      "file": "split_file",
      "ranges": True,
      "output_function": "sort-" + fformat
    })
  else:
    add_function(name, {
      "file": "split_file",
      "format": fformat,
      "split_size": sort_params["chunk_size"],
      "output_function": "pivot-" + fformat,
    }, {
      "ranges": True,
    })

  add_function("sort-" + fformat, {
    "format": fformat,
    "file": "sort",
    "identifier": sort_params["identifier"],
  }, {})

  if sort_params["combine"]:
    add_function("combine-" + fformat, {
      "format": fformat,
      "file": "combine_files",
      "sort": False,
    }, {})

  return functions, pipeline


def process_functions(params):
  pipeline = params["pipeline"]
  variable_to_step = {"input": 0}
  i = 0
  while i < len(pipeline):
    name = pipeline[i]["name"]
    fn_params = params["functions"][name]
    if "output" in pipeline[i]:
      variable_to_step[pipeline[i]["output"]] = i + 1
    if "input" in pipeline[i]:
      pipeline[i]["input_prefix"] = variable_to_step[pipeline[i]["input"]]
    if False:#params["functions"][name]["file"] == "sort":
      del params["functions"][name]
      sort_functions, sort_pipeline = add_sort_pipeline(fn_params)
      pipeline = pipeline[:i] + sort_pipeline + pipeline[i+1:]
      params["functions"] = {**params["functions"], **sort_functions}
      for j in range(len(sort_pipeline)):
        pipeline[i + j - 1]["output_function"] = pipeline[i + j]["name"]
      i += len(sort_pipeline)
    else:
      i += 1
    if i - 1 != len(pipeline) - 1:
      pipeline[i - 1]["output_function"] = pipeline[i]["name"]

    if "output_function" in pipeline[-1]:
      del pipeline[-1]["output_function"]

  params["pipeline"] = pipeline


def upload_functions(client, params):
  response = client.list_functions()
  function_names = set(list(map(lambda f: f["FunctionName"], response["Functions"])))
#  process_functions(params)
  for name in params["functions"]:
    upload_function(client, name, function_names, params)
  for i in range(len(params["pipeline"])):
    print(i, params["pipeline"][i])


def setup_notifications(client, bucket, config):
  print(config)
  response = client.put_bucket_notification_configuration(
      Bucket=bucket,
      NotificationConfiguration=config
  )
  assert(response["ResponseMetadata"]["HTTPStatusCode"] == 200)


def clear_triggers(client, bucket, params):
  setup_notifications(client, bucket, {})


def bucket_exists(client, bucket_name):
  try:
    client.head_bucket(Bucket=bucket_name)
    return True
  except botocore.exceptions.ClientError as ex:
    return False


def create_bucket(client, bucket_name, params):
  if not bucket_exists(client, bucket_name):
    try:
      client.create_bucket(
        ACL="public-read-write",
        Bucket=bucket_name,
        CreateBucketConfiguration={
          "LocationConstraint": params["region"],
        }
      )
    except botocore.exceptions.ClientError as ex:
      if "BucketAlreadyOwnedByYou" not in str(ex):
        raise ex


def function_arns(params):
  name_to_arn = {}
  client = util.lambda_client(params)
  for function in client.list_functions()["Functions"]:
    name_to_arn[function["FunctionName"]] = function["FunctionArn"]
  return name_to_arn


def setup_triggers(params):
  name_to_arn = function_arns(params)

  prefixes = {}
  for name in params["functions"]:
    prefixes[name] = []

  for i in range(len(params["pipeline"])):
    pparams = params["pipeline"][i]
    prefixes[pparams["name"]].append(i)

  client = util.setup_client("s3", params)
  lambda_client = util.lambda_client(params)
  configurations = []
  account_id = boto3.client("sts").get_caller_identity().get("Account")


  for name in [params["pipeline"][0]["name"]]:
    args = {
      "FunctionName": name,
      "StatementId": name + "-" + params["bucket"],
      "Action": "lambda:InvokeFunction",
      "Principal": "s3.amazonaws.com",
      "SourceAccount": account_id,
      "SourceArn": "arn:aws:s3:::{0:s}".format(params["bucket"]),
    }

    try:
      lambda_client.add_permission(**args)
    except Exception:
      pass

    for prefix in [0]:
      configurations.append({
        "LambdaFunctionArn": name_to_arn[name],
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [{"Name": "prefix", "Value": "{0:d}/".format(prefix)}]
          }
        }
      })

      if "suffix" in params["functions"][name]:
        configurations[-1]["Filter"]["Key"]["FilterRules"].append(
          {"Name": "suffix", "Value": params["functions"][name]["suffix"]}
        )

  config = {
    "LambdaFunctionConfigurations": configurations,
  }
  setup_notifications(client, params["bucket"], config)


def setup(params):
  s3 = util.setup_client("s3", params)
  create_bucket(s3, params["bucket"], params)
  create_bucket(s3, params["log"], params)
  clear_triggers(s3, params["bucket"], params)

  client = util.lambda_client(params)
  upload_functions(client, params)
  setup_triggers(params)


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--parameters', type=str, required=True, help="File containing parameters")
  args = parser.parse_args()
  params = json.loads(open(args.parameters).read())
  [access_key, secret_key] = util.get_credentials("default")
  params["access_key"] = access_key
  params["secret_key"] = secret_key
  setup(params)


if __name__ == "__main__":
  main()
