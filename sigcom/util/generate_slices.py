def generate_slices(N, S):
  ''' Slices throughout a range(N) with maximum size S
  e.g.
  generate_slices(10, 4) = (0, 4), (4, 8), (8, 10)
  '''
  for i in range((N // S)+1):
    left, right = i*S, min(S*(i+1), N)
    if left == right: break
    yield left, right
