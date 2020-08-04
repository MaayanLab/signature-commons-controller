def pagination(count, limit=1000):
  offset = 0
  while offset < count:
    yield offset, min(offset + limit, count) - offset
    offset += limit
