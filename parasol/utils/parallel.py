#! /usr/bin/python
#
# parallel utils
#

def mm_mult(ml, mr):
  #line by line, from left to right
  import numpy as np
  for i in xrange(ml.shape[0]):
    for j in xrange(mr.shape[1]):
      yield np.dot(ml[i], mr[:, j])
  
def npfactx(np):
  return np, 1
  
def npfacty(np):
  return 1, np

def npfact2D(np, master = ''):
  import math
  upbnd = int(math.sqrt(np))
  while upbnd - 1:
    if np % upbnd == 0:
      if master == 'col':
        return np / upbnd, upbnd
      # default: row master
      else:
        return upbnd, np / upbnd
    else:
      upbnd -= 1
  if master == 'col':
    return np, 1
  else:
    return 1, np

def decomp2D(mtx, dim1, dim2, a, b, rank):
  px = dim1 / a
  py = dim2 / b
  strow = (rank / b) * px
  stcol = (rank % b) * py
  flag1 = (rank % b == b - 1)
  flag2 = (rank / b == a - 1)
  # right bottom bnd
  if flag1 and flag2:
    return mtx[strow : dim1, stcol : dim2]
  # right bnf
  if flag1:
    return mtx[strow : strow + px, stcol : dim2]
  # bottom bnd
  if flag2:
    return mtx[strow : dim1, stcol : stcol + py]
  # normal block
  return mtx[strow : strow + px, stcol : stcol + py]

def sendrecv(sbuf, sto, stag, rfrom, rtag, comm):
  req = comm.isend(sbuf, dest = sto, tag = stag)
  rbuf = comm.recv(source = rfrom, tag = rtag)
  req.wait()
  return rbuf

def bcastring(sendbuf, func, comm):
  rk = comm.Get_rank()
  sz = comm.Get_size()
  func(sendbuf)
  if sz == 1:  return
  for i in xrange(1, sz):
    f = (rk + i) % sz
    t = (rk + sz - i) % sz
    rbuf = sendrecv(sendbuf, t, 2013, f, 2013, comm)
    func(rbuf) 
 
if __name__ == '__main__':
  import numpy as np
  from scipy import sparse
  ii = np.array([1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 3, 0, 1, 2, 3, 4, 4,
4])
  jj = np.array([5, 4, 0, 4, 1, 2, 3, 4, 3, 4, 0, 1, 2, 6, 6, 6, 6, 2, 4,
6])
  vv = np.array([2, 1, 1, 3, 1, 4, 3, 1, 2, 3, 3, 2, 4, 1, 2, 3, 4, 5, 7,
5])
  mtx = np.array(sparse.coo_matrix((vv, (ii, jj))).todense())
  print mtx
  print npfact2D(18)
  print decomp2D(mtx, 5, 7, 2, 2, 3) 
  np = 12
  rank = 0
  a, b = npfact2D(6)
  submtx = decomp2D(mtx, 5, 7, a, b, rank)
  print submtx
