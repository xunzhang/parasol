#! /usr/bin/python

import time
import sys
sys.path.append('../prototype/')
sys.path.append('/home/xunzhang/xunzhang/Data/book_interest/')
from scipy import sparse
import numpy as np
from mpi4py import MPI
from parallel import *
from clt import kv
import pparse as par

# run on master node
def decomp2D(mtx, u, i, a, b, rank):
  px = u / a
  py = i / b
  strow = (rank / b) * px
  stcol = (rank % b) * py
  #if rank == 3:
  #  print stcol
  flag1 = (rank % b == b - 1)
  flag2 = (rank / b == a - 1)
  # right bottom bnd:
  if flag1 and flag2:
    return mtx[strow : u, stcol : i]
  # right bnd
  if flag1:
    return mtx[strow : strow + px, stcol : i]
  # bottom bnd
  if flag2:
    return mtx[strow : u, stcol : stcol + py]
  # normal block
  return mtx[strow : strow + px , stcol : stcol + py]

def mf_kernel(r, dimu, dimi, p, q, k, rank, b, alpha = 0.0002, beta = 0.02,
steps = 200, conv = 0.0001):
  #threadnum = 0
  q = q.transpose()
  for it in xrange(steps):
    for i in xrange(dimu):
      for j in xrange(dimi):
        if r[i][j] != 0:
          spkey = 'p[' + str(i) + ',:]_' + str(rank / b)
          p[i, :] = kvm.pull(spkey)
          sqkey = 'q[:,' + str(j) + ']_' + str(rank % b)
          q[:, j] = kvm.pull(sqkey)
          eij = r[i][j] - np.dot(p[i, :], q[:, j])
          deltap = []
          deltaq = []
          for ki in xrange(k):
            #p[i][ki] += alpha * (2 * eij * q[ki][j] - beta * p[i][ki])
            #q[ki][j] += alpha * (2 * eij * p[i][ki] - beta * q[ki][j])
            deltap.append(alpha * (2 * eij * q[ki][j] - beta * p[i][ki]))
            deltaq.append(alpha * (2 * eij * p[i][ki] - beta * q[ki][j]))
          #kvm.push(spkey, p[i, :])
          #kvm.push(sqkey, q[:, j])
          kvm.update(spkey, deltap)
          kvm.update(sqkey, deltaq)
          p[i, :] = kvm.pull(spkey)
          q[:, j] = kvm.pull(sqkey)
          #if rank == 3:
          #  threadnum += 4
          #  print 'debug', threadnum
          #time.sleep(0.5)
    esum = 0
    for i in xrange(dimu):
      for j in xrange(dimi):
        if r[i][j] != 0:
          esum += (r[i][j] - np.dot(p[i, :], q[:, j])) ** 2
          for ki in xrange(k):
            esum += (beta / 2) * (p[i][ki] ** 2 + q[ki][j] ** 2)
    esum = comm.allreduce(esum, op = MPI.SUM)
    if rank == 0:
      print it
      print 'local esum is', esum

  return p, q.transpose()
  
def matrix_factorization(r, k, rank, b):
  u = r.shape[0]
  i = r.shape[1]
  p = np.random.rand(u, k)
  q = np.random.rand(i, k)
  #kvm.push('p[0,:]_0', [0.65918662, 0.65221886])
  #kvm.push('p[1,:]_0', [0.72454662, 0.5592554])
  #kvm.push('p[2,:]_0', [0.0513879, 0.53047435])
  #kvm.push('p[3,:]_0', [0.80869252, 0.37988651])
  #kvm.push('p[4,:]_0', [0.4690092, 0.08531032])
  #kvm.push('q[:,0]_0',[0.40330888, 0.61473888])
  #kvm.push('q[:,1]_0',[0.35554828, 0.95415641])
  #kvm.push('q[:,2]_0',[0.52891785, 0.46115004])
  #kvm.push('q[:,3]_0',[0.21381893, 0.34623335])
  #kvm.push('q[:,4]_0',[0.1612169, 0.76223091])
  #kvm.push('q[:,5]_0',[0.77667506, 0.71933245])
  #kvm.push('q[:,6]_0',[0.42349893, 0.73313934])
  # push p0
  for index in xrange(u):
    key = 'p[' + str(index) + ',:]_' + str(rank / b)
    kvm.push(key, list(p[index, :]))
  # push q0
  for index in xrange(i):
    key = 'q[:,' + str(index) + ']_' + str(rank % b)
    kvm.push(key, list(q[index, :]))
  # kernel mf solver
  p, q = mf_kernel(r, u, i, p, q, k, rank, b)
  
  return p, q

if __name__ == '__main__':
  comm = MPI.COMM_WORLD
  k = 20
  filename = '/home/xunzhang/xunzhang/Data/book_interest/001.csv'
  pavg = par.avg(filename, 'P')
  uid_lst, sid_lst, rating = par.init(filename, 'P', pavg)
  dx, dy, rating_matrix = par.create_matrix(uid_lst, sid_lst, rating)
  rating_mtx = par.gen_sparse(rating_matrix)
  mtx = np.array(rating_mtx.todense())
  u = mtx.shape[0]
  i = mtx.shape[1]
  print u
  print i 
  #ii = np.array([1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 3, 0, 1, 2, 3, 4, 4, 4])
  #jj = np.array([5, 4, 0, 4, 1, 2, 3, 4, 3, 4, 0, 1, 2, 6, 6, 6, 6, 2, 4, 6])
  #vv = np.array([2, 1, 1, 3, 1, 4, 3, 1, 2, 3, 3, 2, 4, 1, 2, 3, 4, 5, 7, 5])
  #mtx = np.array(sparse.coo_matrix((vv, (ii, jj))).todense())
  #u = 5
  #i = 7
  #k = 2
  # get rank
  rank = comm.Get_rank()
  # divide mtx
  a, b = npfact2D(16)
  mtx = decomp2D(mtx, u, i, a, b, rank)
  kvm = kv('localhost', 7900)
  if rank == 0:
    print 'init done'
  p, q = matrix_factorization(mtx, k, rank, b)
  #if rank == 3:
  #  print mtx
  #  print np.dot(p, q.T)
  if rank == 0:
    print 'result 5.0'
    print np.dot(p, q.T)[56][65]
  # merge p and q
