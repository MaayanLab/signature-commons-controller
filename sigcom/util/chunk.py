def chunk(iterable, limit=1000):
  buffer = []
  for i, element in enumerate(iterable, start=1):
    buffer.append(element)
    if i % limit == 0:
      yield buffer
      buffer = []
  if buffer:
    yield buffer
