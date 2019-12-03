import numpy as np
import util
from database import Database
from PIL import Image
from typing import List


def make_file(output_format):
  output_format["bin"] += 1
  util.make_folder(output_format)
  name = util.file_name(output_format)
  output_file = "/tmp/" + name
  f = open(output_file, "wb+")
  return [f, name]


def create_window(im, x, y, window_width, window_height, width, height):
  window = np.zeros([2 * window_height + 1, 2 * window_width + 1, 3], dtype=int)
  for index_y in range(2 * window_height + 1):
    window_y = y - window_height + index_y
    if 0 <= window_y and window_y < height:
      for index_x in range(2 * window_width + 1):
        window_x = x - window_width + index_x
        if 0 <= window_x and window_x < width:
          window[index_y][index_x] = im[window_x, window_y]

  return window


def run(database: Database, key: str, params, input_format, output_format, offsets: List[int]):
  im = Image.open(key)
  px = im.load()
  width, height = im.size
  output_format["ext"] = "pixel"

  num_bins = int((width * height + params["pixels_per_bin"] - 1) / params["pixels_per_bin"])
  output_format["bin"] = 0
  output_format["num_bins"] = num_bins
  [f, name] = make_file(output_format)

  for y in range(height):
    for x in range(width):
      if y != 0 or x != 0:
        f.write(b'\n\n')
      window = create_window(px, x, y, 1, 1, width, height)
      f.write(str.encode("{x} {y} ".format(x=x, y=y)) + window.tostring())
      if (y * width + x) % params["pixels_per_bin"] == params["pixels_per_bin"] - 1:
        f.close()
        database.put(params["bucket"], name, open("/tmp/" + name, "rb"), {})
        [f, name] = make_file(output_format)
  f.close()
  database.put(params["bucket"], name, open("/tmp/" + name, "rb"), {})
  assert(num_bins == output_format["bin"])
  return []
