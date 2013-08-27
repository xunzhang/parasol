#! /usr/bin/python
# 
#
#
#
#

import sys
from scipy import sparse
import numpy as np

# calc avg of rating with the same ind
def avg(fname, ind):
  f = open(fname)
  rsum = 0
  rcnt = 0
  for line in f:
    l = line.strip('\n').split('\t')
    if l[2] == ind:
      if l[3] != 'NULL' and l[3] != '':
        r = int(l[3])
        if r >= 0 and r <=5:
          rsum += r
          rcnt += 1
  return float(rsum) / float(rcnt)

# init uid_lst, sid_lst, rating_lst from data file
def init(fname, ind, avg):
  uid_lst = []
  sid_lst = []
  rating = []
  f = open(fname)
  for line in f:
    l = line.strip('\n').split('\t')
    if l[2] == ind:
      uid_lst.append(int(l[0]))
      sid_lst.append(int(l[1]))
      if l[3] == 'NULL' or l[3] == '':
        rating.append(avg)
      else:
        try:
            rating.append(int(l[3]))
        except:
            print int(l[3])
            sys.exit(0)
  return uid_lst, sid_lst, rating 

# create rating_matrix(nonzero entities)
# mapping original u, s to dense indexes(info contained in dictx and dicty)
# raing_matrix: (line index of u, line index of s) -> rating
def create_matrix(luid, lsid, lr):
  dictx = {}
  dicty = {}
  setuid = list(set(luid))
  setsid = list(set(lsid))
  cnt = 0
  for i in setuid:
    dictx[i] = cnt
    cnt += 1
  cnt = 0
  for j in setsid: 
    dicty[j] = cnt
    cnt += 1
  rdict = {}
  for i in xrange(len(luid)):
    rdict[(dictx[luid[i]], dicty[lsid[i]])] =  lr[i]
  return dictx, dicty, rdict 

# get original ou and os from mapped u and s
def mapping_original(u, s, dx, dy):
  for k in dx.keys():
    if dx[k] == u:
      ou = k
  for k in dy.keys():
    if dy[k] == s:
      os = k
  return ou, os

def gen_sparse(rating_mtx):
  i = []
  j = []
  v = []
  for key in rating_mtx.keys():
    i.append(key[0])
    j.append(key[1])
    v.append(rating_mtx[key])
  rmtx = sparse.coo_matrix((np.array(v), (np.array(i), np.array(j))))
  return rmtx
 
# print k key-val from rating_matrix, index is mapped
def print_k(rating_matrix, k):
  cnt = 0
  for i in rating_matrix.keys():
    if cnt < k:
      print i, '-->', rating_matrix[i]
      cnt += 1

# check rating using original u and s
def check_rating(fname, u, s, r):
  f = open(fname)
  for line in f:
    l = line.strip('\n').split('\t')
    if int(l[0]) == u and int(l[1]) == s:
      if int(l[3]) == r:
        return True
      else:
        return False

if __name__ == '__main__':
  filename = '001.csv'

  pavg = avg(filename, 'P')
  #favg = avg(filename, 'F')
  #navg = avg(filename, 'N')
  print 'pavg done' 
  uid_lst, sid_lst, rating = init(filename, 'P', pavg)
  print 'init done'
  dx, dy, rating_matrix = create_matrix(uid_lst, sid_lst, rating)
  print 'create_matrix done'
  rating_mtx = gen_sparse(rating_matrix)
  print 'gen_sparse done'
  #print rating_mtx
  #print rating_matrix
  #print_k(rating_matrix, 10)
  #print mapping_original(506, 11407, dx, dy)
  #print check_rating(filename, 43878370, 5340278, 4)
