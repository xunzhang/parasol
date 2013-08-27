#! /usr/bin/python
#
# parallel loading lines from file(s) and generate block matrix
#

import numpy as np
from scipy.sparse import coo_matrix

def ge_blkmtx(fl, comm, fmt = 'ussrt'):
  '''
  Parallel loading lines from fl and generate block matrix

  Parameters
  ----------
  fl : file ident
  fmt : file format
  comm : communication scope
  
  Returns
  -------
  rmap : row mapping from ids to inds
  cmap : col mapping from ids to inds
  mtx : block matrix in coo fmt
  
  Examples
  --------
  >>> from mpi4py import MPI
  >>> comm = MPI.COMM_WORLD
  >>> rank = comm.Get_rank()
  >>> fl = '/home/xunzhang/demo/'
  >>> rmap, cmap, mtx = ge_blkmtx(fl, comm)
  >>> if rank == 0:
  >>>   print rmap
  >>>   print cmap
  >>>   print mtx
  '''
  
  from expand import expd_f_lst
  from load import fns_partition
  from np_scheduler import scheduler_load, exchange
  from hashtransfer import putlines
  from toinds import ind_mapping
   
  # expand fl to fns
  fns = expd_f_lst(fl)
  rank = comm.Get_rank()
  sz = comm.Get_size()

  # generate load list
  loads = fns_partition(fns, sz)
  print 'rank %d loads finished' % rank

  # parallel loading lines
  lines = scheduler_load(comm, loads)
  print 'rank %d lines got' % rank

  # hash lines into slotslt
  slotslst = putlines(lines, sz, fmt)
  print 'rank %d slotslst generated' % rank

  comm.barrier()

  # alltoall exchange, get desirable lines
  slotslst = exchange(slotslst, comm)
  print 'rank %d get desirable lines' % rank

  comm.barrier()

  # mapping inds to ids and get rmap, cmap, new slotslst((rid, cid, val)s)
  rmap, cmap, slotslst = ind_mapping(slotslst, comm)
  print 'finish ind_mapping'

  # generate block matrix
  mtx = coo_matrix((np.array([i[2] for i in slotslst]), (np.array([i[0] for i in slotslst]), np.array([i[1] for i in slotslst]))))
  
  return rmap, cmap, mtx
