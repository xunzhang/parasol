def traverse(it,dim1):
  rc = 0
  cc = 0
  for stf in it:
    print rc, cc, stf
    cc += 1
    if cc == dim1:
      rc += 1
      cc = 0

def mm_mult(ml, mr):
  import numpy as np
  for i in xrange(ml.shape[0]):
    for j in xrange(mr.shape[1]):
      yield np.dot(ml[i], mr[:, j])

if __name__ == '__main__':
  import numpy as np
  p = np.random.rand(4, 2)
  q = np.random.rand(2, 3)
  print p
  print q
  print np.dot(p, q)
  it = mm_mult(p, q) 
  traverse(it, q.shape[1])
