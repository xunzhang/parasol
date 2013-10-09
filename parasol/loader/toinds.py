#! /usr/bin/python
#
# transfer uids and subject_ids to row and col indexes
#

from mpi4py import MPI

def ind_mapping(slotslst, comm, pattern = 'fmap'):
  '''
  mapping inds to ids and return the mapping from ids to inds
  
  Parameters
  ----------
  slotslst : list of (rowind, colind, val)s
  comm : communication scope
  
  Returns
  -------
  rowmap : row map from ids to inds 
  colmap : col map from ids to inds
  slotslst : list of (rowid, colid, val)s
  
  Examples:
  --------
  >>> comm = MPI.COMM_WORLD
  >>> rank = comm.Get_rank()
  >>> slotslst = []
  >>> if rank == 0:
  >>>   slotslst = [(64, 6, 3), (64, 42, 4)]
  >>> if rank == 1:
  >>>   slotslst = [(64, 21, 1)]
  >>> if rank == 2:
  >>>   slotslst = [(27, 28, 1), (27, 42, 2), (37, 42, 3), (29, 28, 1), (29, 6, 3)]
  >>> if rank == 3:
  >>>   slotslst = [(37, 21, 5)]
  >>> rm, cm, slotslst = ind_mapping(slotslst, comm)
  >>> if rank == 2:
  >>>   print rm
  >>>   print cm
  >>>   print slotslst
  '''
  import itertools
  from parasol.utils.parallel import npfactx, npfacty, npfact2D
  np = comm.Get_size()
  rank = comm.Get_rank()

  npx, npy = npfactx(np)
  if pattern == 'fsmap':
    npx, npy = npfact2D(np)
  if pattern == 'smap':
    npx, npy = npfacty(np)
  
  rowcolor = rank / npy
  colcolor = rank % npy
  row_comm = comm.Split(colcolor, rank)
  col_comm = comm.Split(rowcolor, rank)
  
  rows = [stf[0] for stf in slotslst] 
  cols = [stf[1] for stf in slotslst] 
 
  # ['a', 'b', 'a', 'a', 'c', 'b'] -> ['a', 'a', 'a', 'b', 'b', 'c'] -> [3, 2, 1]
  degree = [len(list(grp)) for key, grp in itertools.groupby(rows)]  
  degree.sort()
  degreemap = {}
  for item in degree:
    degreemap[degree.index(item)] = item
  
  cols = list(set(row_comm.allreduce(cols, op = MPI.SUM)))
  rows = list(set(col_comm.allreduce(rows, op = MPI.SUM)))
  rows.sort()
  cols.sort()
  rowmap = {}
  rowrevmap = {}
  colmap = {}
  colrevmap = {}
  for ids, ind in enumerate(rows):
    rowmap[ids] = ind
    rowrevmap[ind] = ids
  for ids, ind in enumerate(cols):
    colmap[ids] = ind
    colrevmap[ind] = ids
  for k in xrange(len(slotslst)):
    slotslst[k] = (rowrevmap[slotslst[k][0]], colrevmap[slotslst[k][1]], slotslst[k][2])
  
  return rowmap, colmap, slotslst, degreemap
 
if __name__ == '__main__': 
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  slotslst = []
  if rank == 0:
    slotslst = [(64, 6, 3), (64, 42, 4)]
  if rank == 1:
    slotslst = [(64, 21, 1)]
  if rank == 2:
    slotslst = [(27, 28, 1), (27, 42, 2), (37, 42, 3), (29, 28, 1), (29, 6, 3)]
  if rank == 3:
    slotslst = [(37, 21, 5)]
  rm, cm, slotslst, dm = ind_mapping(slotslst, comm)
  if rank == 2:
    print rm
    print cm
    print dm
    print slotslst

  slotslst2 = []
  if rank == 0:
    slotslst2 = [('a', 'b', 1), ('a', 'c', 1), ('a', 'd', 1), ('b', 'd', 1), ('b', 'a', 1)]
  if rank == 1:
    slotslst2 = [('d', 'c', 1), ('c', 'b', 1), ('c', 'd', 1)]
  rm2, cm2, slotslst2, dm2 = ind_mapping(slotslst2, comm)
  if rank == 0:
    print rm2
    print cm2
    print dm2
    print slotslst2
