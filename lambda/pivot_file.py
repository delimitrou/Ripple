import boto3
import importlib
import util
from database import Database, Entry
from iterator import OffsetBounds
from typing import Any, Dict, List


def create_pivots(d: Database, format_lib: Any, iterator_class: Any, items: List[Any], params: Dict[str, Any]) -> List[float]:
  if len(items) == 0:
    return []

  pivots: List[float] = map(lambda item: iterator_class.get_identifier_value(item, format_lib.Identifiers[params["identifier"]]), items)
  pivots = list(set(list(pivots)))
  pivots.sort()

  max_identifier: float = float(pivots[-1] + 1)
  num_bins = 64
  #num_bins = 8
  increment = int((len(items) + num_bins - 1) / num_bins)
  pivots = pivots[0::increment]
  if pivots[-1] == max_identifier - 1:
    pivots[-1] = max_identifier
  else:
    pivots.append(max_identifier)
  return pivots


def handle_pivots(database: Database, bucket_name, key, input_format, output_format, offsets, params):
  entry: Entry = database.get_entry(bucket_name, key)

  format_lib = importlib.import_module(params["input_format"])
  iterator_class = getattr(format_lib, "Iterator")
  if len(offsets) > 0:
    it = iterator_class(entry, OffsetBounds(offsets[0], offsets[1]))
  else:
    it = iterator_class(entry, None)

  items = it.get(it.get_start_index(), it.get_end_index())
  pivots: List[float] = create_pivots(database, format_lib, iterator_class, list(items), params)

  output_format["ext"] = "pivot"
  pivot_key = util.file_name(output_format)

  spivots = "\t".join(list(map(lambda p: str(p), pivots)))
  content = str.encode("{0:s}\n{1:s}\n{2:s}".format(bucket_name, key, spivots))
  database.write(params["bucket"], pivot_key, content, {})
  return True


def handler(event, context):
  util.handle(event, context, handle_pivots)
