import classification
import knn
import math
import numpy as np
import util
from iterator import OffsetBounds
from sklearn.neighbors import NearestNeighbors
from database import Database
from typing import List


def __distance__(p, clz):
  [_, _, pr, pg, pb] = p
  [cr, cg, cb, cc] = clz
  return math.sqrt((cr - pr) ** 2 + (cg - pg) ** 2 + (cb - pb) ** 2)


def run(database: Database, test_key: str, params, input_format, output_format, offsets: List[int]):
  train_obj = database.get_entry("spacenet", params["train_key"])
  train_it = classification.Iterator(train_obj, OffsetBounds(params["train_offsets"][0], params["train_offsets"][1]))
  train_x = []
  train_y = []
  more = True
  while more:
    [items, _, more] = train_it.next()
    for [features, c] in items:
      train_x.append(features)
      train_y.append(c)

  neigh = NearestNeighbors(n_neighbors=params["k"], algorithm="brute")
  neigh.fit(train_x)

  pixels = []
  rgb = []
  with open(test_key, "rb") as f:
    lines = filter(lambda l: len(l.strip()) > 0, f.read().split(b"\n\n"))
    for line in lines:
      parts = line.split(b' ')
      x = int(parts[0])
      y = int(parts[1])
      pixels.append([x, y])
      rgb.append(np.frombuffer(b' '.join(parts[2:]), dtype=int))

  [distances, indices] = neigh.kneighbors(rgb)

  items = []
  for i in range(len(distances)):
    [x, y] = pixels[i]
    neighbors = []
    for j in range(len(distances[i])):
      distance = distances[i][j]
      clz = train_y[indices[i][j]]
      neighbors.append((distance, clz))
    items.append((str.encode("{x} {y}".format(x=x, y=y)), neighbors))

  output_format["ext"] = "knn"
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  with open(output_file, "wb+") as f:
    knn.Iterator.from_array(items, f, {})

  return [output_file]
