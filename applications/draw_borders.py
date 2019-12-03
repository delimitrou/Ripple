import knn
import numpy as np
import util
from database import Database
from enum import Enum
from PIL import Image
from typing import List


class Indices(Enum):
  inside = 0
  border = 1
  outside = 2


def flood(classifications, x, y, visited, width, height, cl):
  stack = [(x, y)]

  while len(stack) > 0:
    [x, y] = stack.pop(0)
    if x >= width or y >= height:
      continue
    if x < 0 or y < 0:
      continue
    if x in visited and y in visited[x]:
      continue
    if x not in visited:
      visited[x] = set()
    visited[x].add(y)

    # Check if border, if so stop
    if classifications[y][x] == Indices.border.value:
      continue

    counts = [0, 0, 0]
    total = 0
    for dy in range(-1, 2):
      for dx in range(-1, 2):
        iy = y + dy
        ix = x + dx
        if (0 <= iy and iy < height) and (0 <= ix and ix < width):
          counts[classifications[iy][ix]] += 1
          total += 1

    if counts[cl] > (3 * total / 4.0):
      classifications[y][x] = cl
    stack.append((x - 1, y - 1))
    stack.append((x - 1, y))
    stack.append((x - 1, y + 1))
    stack.append((x, y - 1))
    stack.append((x, y + 1))
    stack.append((x + 1, y - 1))
    stack.append((x + 1, y))
    stack.append((x + 1, y + 1))


def run(database: Database, key: str, params, input_format, output_format, offsets: List[int]):
  prefix = "{0:d}/{1:f}-{2:d}/".format(params["image"], input_format["timestamp"], input_format["nonce"])
  entries = database.get_entries(params["bucket"], prefix)
  assert(len(entries) == 1)
  entry = entries[0]

  output_format["ext"] = entry.key.split(".")[-1]
  output_file = "/tmp/{0:s}".format(util.file_name(output_format))
  with open(output_file, "wb+") as f:
    entry.download(f)

  im = Image.open(output_file)
  width, height = im.size

  classifications = np.empty([height, width], dtype=int)
  it = knn.Iterator(database.get_entry(params["bucket"], key.replace("/tmp/", "")))
  more = True
  while more:
    [items, _, more] = it.next()
    for [point, neighbors] in items:
      [x, y] = list(map(lambda p: int(p), point.split(b' ')))
      scores = [0, 0, 0]
      neighbors = sorted(neighbors, key=lambda n: n[0])
      d1 = neighbors[0][0]
      dk = neighbors[-1][0]
      denom = dk - d1 if dk != d1 else 0.0001
      for i in range(len(neighbors)):
        [d, c] = neighbors[i]
        w = 1 if i == 0 else (dk - d) / denom
        scores[c] += w
      m = max(scores)
      top = [i for i, j in enumerate(scores) if j == m]
      if len(top) == 1:
        classifications[y][x] = top[0]
      else:
        classifications[y][x] = Indices.outside.value

  for y in range(height):
    for x in range(width):
      if classifications[y][x] == Indices.border.value:
        counts = [0, 0, 0]
        for dy in range(-1, 2):
          for dx in range(-1, 2):
            iy = y + dy
            ix = x + dx
            if (0 <= iy and iy < height) and (0 <= ix and ix < width):
              counts[classifications[iy][ix]] += 1
        if counts[Indices.border.value] <= 1:
          classifications[y][x] = Indices.outside.value

#  visited = {}
#  for y in range(height):
#    for x in range(width):
#      if x in visited and y in visited[x]:
#        continue
#
#      if classifications[y][x] == Indices.outside.value:
#        counts = [0, 0, 0]
#        total = 0
#        for dy in range(-1, 2):
#          for dx in range(-1, 2):
#            iy = y + dy
#            ix = x + dx
#            if (0 <= iy and iy < height) and (0 <= ix and ix < width):
#              counts[classifications[iy][ix]] += 1
#            total += 1
#        if counts[Indices.outside.value] == total:
#          flood(classifications, x, y, visited, width, height, Indices.outside.value)

  for y in range(height):
    for x in range(width):
      if classifications[y][x] == Indices.border.value:
        im.putpixel((x, y), (255, 0, 0))
#      elif classifications[y][x] == Indices.inside.value:
#        im.putpixel((x, y), (255, 255, 0))

  im.save(output_file)

  return [output_file]
