import boto3
import botocore
import json
import os
import random
import time
from typing import Any, BinaryIO, Dict, List, Optional, Union


class Statistics:
  list_count: int
  read_byte_count: int
  read_count: int
  write_byte_count: int
  write_count: int

  def __init__(self):
    self.list_count = 0
    self.read_byte_count = 0
    self.read_count = 0
    self.write_byte_count = 0
    self.write_count = 0

  def calculate_total_cost(self) -> float:
    list_cost: float = (self.list_count / 1000.0) * 0.005
    read_cost: float = (self.read_count / 1000.0) * 0.004
    write_cost: float = (self.write_count / 1000.0) * 0.005
    print("List cost:", list_cost)
    print("Read cost:", read_cost)
    print("Write cost:", write_cost)
    total_cost: float = list_cost + read_cost + write_cost
    print("Total cost:", total_cost)
    return total_cost


class Entry:
  key: str
  resources: Any
  statistics: Optional[Statistics]

  def __init__(self, key: str, resources: Any, statistics: Optional[Statistics]):
    self.key = key
    self.resources = resources
    self.statistics = statistics

  def __download__(self, f: BinaryIO) -> int:
    raise Exception("Entry::__download__ not implemented")

  def __get_content__(self) -> bytes:
    raise Exception("Entry::__get_content__ not implemented")

  def __get_range__(self, start_index: int, end_index: int) -> bytes:
    raise Exception("Entry::get_range not implemented")

  def content_length(self) -> int:
    raise Exception("Entry::content_length not implemented")

  def download(self, f: BinaryIO) -> int:
    count = 0
    done = False
    while not done:
      self.statistics.read_count += 1
      try:
        content_length: int = self.__download__(f)
        return content_length
      except Exception as e:
        count += 1
        if count == 3:
          raise e

  def get_content(self) -> bytes:
    self.statistics.read_count += 1
    return self.__get_content__()

  def get_metadata(self) -> Dict[str, str]:
    raise Exception("Entry::get_metadata not implemented")

  def get_range(self, start_index: int, end_index: int) -> bytes:
    self.statistics.read_count += 1
    return self.__get_range__(start_index, end_index)

  def last_modified_at(self) -> float:
    raise Exception("Entry::last_modified_at not implemented")


class Table:
  name: str
  resources: Any
  statistics: Statistics

  def __init__(self, name: str, statistics: Statistics, resources: Any):
    self.name = name
    self.resources = resources
    self.statistics = statistics


class Database:
  payloads: List[Dict[str, Any]]
  statistics: Statistics

  def __init__(self):
    self.payloads = []
    self.statistics = Statistics()
    self.max_sleep_time = 5

  def __download__(self, table_name: str, key: str, f: BinaryIO) -> int:
    raise Exception("Database::__download__ not implemented")

  def __get_entries__(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    raise Exception("Database::__get_entries__ not implemented")

  def __put__(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str]):
    raise Exception("Database::__put__ not implemented")

  def __write__(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str]):
    raise Exception("Database::__write__ not implemented")

  def contains(self, table_name: str, key: str) -> bool:
    raise Exception("Database::contains not implemented")

  def create_payload(self, table_name: str, key: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    raise Exception("Database::create_payload not implemented")

  def download(self, table_name: str, key: str, file_name: str) -> int:
    count = 0
    done = False
    while not done:
      self.statistics.read_count += 1
      try:
        with open(file_name, "wb+") as f:
          content_length: int = self.__download__(table_name, key, f)
        return content_length
      except Exception as e:
        count += 1
        if count == 3:
          raise e

  def get_entry(self, table_name: str, key: str) -> Optional[Entry]:
    raise Exception("Database::key not implemented")

  def get_entries(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    self.statistics.list_count += 1
    return self.__get_entries__(table_name, prefix)

  def get_statistics(self) -> Dict[str, Any]:
    return {
      "payloads": self.payloads,
      "read_count": self.statistics.read_count,
      "write_count": self.statistics.write_count,
      "list_count": self.statistics.list_count,
      "write_byte_count": self.statistics.write_byte_count,
      "read_byte_count": self.statistics.read_byte_count,
    }

  def get_table(self, table_name: str) -> Table:
    raise Exception("Database::get_table not implemented")

  def invoke(self, name, payload):
    raise Exception("Database::invoke not implemented")

  def put(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str], invoke=True):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += os.path.getsize(content.name)
    self.__put__(table_name, key, content, metadata, invoke)

  def read(self, table_name: str, key: str) -> bytes:
    self.statistics.read_count += 1
    content: bytes = self.__read__(table_name, key)
    self.statistics.read_byte_count += len(content)
    return content

  def write(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str], invoke=True):
    self.statistics.write_count += 1
    self.statistics.write_byte_count += len(content)
    self.__write__(table_name, key, content, metadata, invoke)


class Object(Entry):
  def __init__(self, key: str, resources: Any, statistics: Statistics):
    Entry.__init__(self, key, resources, statistics)

  def __download__(self, f: BinaryIO) -> int:
    self.resources.download_fileobj(f)
    return f.tell()

  def __get_content__(self) -> bytes:
    return self.resources.get()["Body"].read()

  def __get_range__(self, start_index: int, end_index: int) -> bytes:
    return self.resources.get(Range="bytes={0:d}-{1:d}".format(start_index, end_index))["Body"].read()

  def content_length(self) -> int:
    return self.resources.content_length

  def get_metadata(self) -> Dict[str, str]:
    return self.resources.metadata

  def last_modified_at(self) -> float:
    return self.resources.last_modified.timestamp()


class Bucket(Table):
  def __init__(self, name: str, statistics: Statistics, resources: Any):
    Table.__init__(self, name, statistics, resources)
    self.bucket = self.resources.Bucket(name)


class S3(Database):
  def __init__(self, params):
    self.s3 = boto3.resource("s3")
    self.client = boto3.client("lambda")
    self.params = params
    self.sleep_time = 1
    Database.__init__(self)

  def __download__(self, table_name: str, key: str, f: BinaryIO) -> int:
    bucket = self.s3.Bucket(table_name)
    bucket.download_fileobj(key, f)
    return f.tell()

  def __get_content__(self, table_name: str, key: str, start_byte: int, end_byte: int) -> bytes:
    obj = self.s3.Object(table_name, key)
    return obj.get(Range="bytes={0:d}-{1:d}".format(start_byte, end_byte))["Body"].read()

  def __get_entries__(self, table_name: str, prefix: Optional[str]=None) -> List[Entry]:
    done = False
    bucket = self.s3.Bucket(table_name)
    while not done:
      try:
        if prefix:
          objects = bucket.objects.filter(Prefix=prefix)
        else:
          objects = bucket.objects.all()
        objects = list(map(lambda obj: Object(obj.key, self.s3.Object(table_name, obj.key), self.statistics), objects))
        done = True
        self.sleep_time = min(max(int(self.sleep_time / 2), 1), self.max_sleep_time)
      except Exception as e:
        time.sleep(self.sleep_time)
        self.sleep_time *= 2

    return objects

  def __put__(self, table_name: str, key: str, content: BinaryIO, metadata: Dict[str, str], invoke=True):
    self.__s3_write__(table_name, key, content, metadata)

  def __read__(self, table_name: str, key: str) -> bytes:
    obj = self.s3.Object(table_name, key)
    content = obj.get()["Body"].read()
    return content

  def __write__(self, table_name: str, key: str, content: bytes, metadata: Dict[str, str], invoke=True):
    self.__s3_write__(table_name, key, content, metadata, invoke)

  def __s3_write__(self, table_name: str, key: str, content: Union[bytes, BinaryIO], metadata: Dict[str, str], invoke=True):
    done: bool = False
    while not done:
      try:
        self.s3.Object(table_name, key).put(Body=content, Metadata=metadata)
        self.sleep_time = min(max(int(self.sleep_time / 2), 1), self.max_sleep_time)
        done = True
      except botocore.exceptions.ClientError as e:
        print("Warning: S3::write Rate Limited. Sleeping for", self.sleep_time)
        time.sleep(self.sleep_time)
        self.sleep_time *= 2

    if "output_function" in self.params and invoke:
      payload = {
        "Records": [{
          "s3": {
            "bucket": {
              "name": table_name
            },
            "object": {
              "key": key
            },
            "ancestry": self.params["ancestry"],
          },
        }]
      }
      if "reexecute" in self.params:
        payload["execute"] = self.params["reexecute"]
      self.payloads.append(payload)
      self.invoke(self.params["output_function"], payload)

  def contains(self, table_name: str, key: str) -> bool:
    try:
      self.s3.Object(table_name, key).load()
      return True
    except Exception:
      return False

  def get_entry(self, table_name: str, key: str) -> Optional[Object]:
    return Object(key, self.s3.Object(table_name, key), self.statistics)

  def get_table(self, table_name: str) -> Table:
    return Table(table_name, self.statistics, self.s3)

  def create_payload(self, table_name: str, key: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
      "Records": [{
        "s3": {
          "bucket": {
            "name": table_name
          },
          "object": {
            "key": key
          },
          "extra_params": extra,
          "ancestry": self.params["ancestry"],
        }
      }]
    }

    if "reexecute" in self.params:
      payload["execute"] = self.params["reexecute"]
    return payload

  def invoke(self, name, payload):
    self.payloads.append(payload)
    response = self.client.invoke(
      FunctionName=name,
      InvocationType="Event",
      Payload=json.JSONEncoder().encode(payload)
    )
    assert(response["ResponseMetadata"]["HTTPStatusCode"] == 202)
