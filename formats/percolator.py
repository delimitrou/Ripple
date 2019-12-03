import tsv
import util


class Iterator(tsv.Iterator):
  QVALUE_INDEX = 7
  THRESHOLD = 0.01

  def __init__(self, obj, chunk_size):
    tsv.Iterator.__init__(self, obj, chunk_size)
    self.cls = Iterator

  def getQValue(line):
    return float(line.split(tsv.Iterator.COLUMN_SEPARATOR)[Iterator.QVALUE_INDEX])

  def get(obj, start_byte, end_byte, identifier=""):
    content = util.read(obj, start_byte, end_byte)
    lines = list(content.split(tsv.Iterator.IDENTIFIER))

    if identifier == "q-value":
      lines = list(filter(lambda line: len(line.strip()) > 0, lines))
      lines = list(map(lambda line: (Iterator.getQValue(line), line), lines))
    elif identifier != "":
      raise Exception("Unknown identifier for percolator format", identifier)

    return lines

  def sum(self, identifier):
    more = True
    count = 0
    while more:
      [lines, more] = self.next(identifier)
      for line in lines:
        if line[0] <= Iterator.THRESHOLD:
          count += 1
    return count
