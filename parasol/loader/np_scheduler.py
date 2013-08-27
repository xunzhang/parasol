#! /usr/bin/python
#
# Scheduler for np proces loading lines
#

import sys
from mpi4py import MPI
import threading

# default BLK_SZ is 8, total tasks must be np * BLK_SZ
# use BLK_SZ to make np = 1 still works(to test performance)
BLK_SZ = 8

mutex = threading.Lock()

def scheduler_load(comm, loads, host = 0):
  '''
  Schedule loading lines for each proces

  Notes
  -----
  By default, BLK_SZ is 8. So there are np * 8 tasks in total.
  Procs host loads lines from loads[0-7] while other procs loads lines in unordered way

  Parameters
  ----------
  comm : communicator of MPI prog  
  loads : loads list generated from fns_partition function in load.py  
  host : host procs, 0 by default
  
  Returns
  -------
  ret : lines list
  
  Examples
  --------
  >>> comm = MPI.COMM_WORLD
  >>> rank = comm.Get_rank()
  >>> sz = comm.Get_rank()
  >>> from load import fns_partition
  >>> loads = fns_partition(['a.txt'], sz)
  >>> lst = scheduler_load(comm, loads)
  >>> if rank == 0:
  >>>   print lst
  '''
  ret = []
  rank = comm.Get_rank()
  size = comm.Get_size()
  cnt = BLK_SZ - 1
  cntcontrol = BLK_SZ
  ntasks = size * BLK_SZ
  bnd = ntasks + size - 2
  flag = 0

  if rank == host:
    # load tasks [0,BLK_SZ-1]
    for i in xrange(BLK_SZ):
      lines = loads[i]()
      for line in lines:
         ret.append(line)
  if rank != host:
    while True:
      if flag == 1: break
      if cnt == ntasks - 1: break
      comm.send(rank, host, 2013)
      cnt = comm.recv(source = host, tag = 2013)
      flag = comm.recv(source = host, tag = 2013)
      #print 'rank %d received from host with cnt val %d' % (rank, cnt)
      if flag == 0:
        # loading lines
        lines = loads[cnt]()
        for line in lines:
          ret.append(line)
  else:
    while cntcontrol < bnd:
      cntcontrol += 1
      global mutex
      mutex.acquire()
      stat = MPI.Status()
      tmp = comm.recv(source = MPI.ANY_SOURCE, tag = MPI.ANY_TAG, status = stat)
      if flag == 0:
        cnt += 1
      src = stat.Get_source()
      comm.send(cnt, src, 2013)
      if cnt == ntasks - 1 and flag == 0:
        comm.send(flag, src, 2013)
        flag = 1
      else:
        comm.send(flag, src, 2013)
      mutex.release()
  return ret

def exchange(slotslst, comm):
  '''
  Exchange stuff and get relevant block stuff
  
  Note
  ----
  len(slotslst) must be equal to number of procs
  
  Parameters
  ----------
  slotslst : a list of list, generated from putlines function in hashtransfer.py
  comm : communication scope
   
  Return
  ------
  list of (i, j, v) entites relevant block matrix
  
  '''
  recvobj = comm.alltoall(slotslst)
  stf = []
  for item in recvobj:
    stf += item
  return stf
  #ret = comm.allgather(stf, slotslst)

if __name__ == '__main__':
  comm = MPI.COMM_WORLD
  from load import fns_partition
  rank = comm.Get_rank()
  sz = comm.Get_size()
  loads = fns_partition(['a.txt'], sz)
  print len(loads)
  aa = scheduler_load(comm, loads)
  if rank == 0:
    print aa
  ll = [[], [], [], []]
  # example for exchange function with np = 4
  if rank == 0:
    ll = [[(1, 1, 1) ,(2, 2, 2)], [(3, 3, 3)], [], [(4, 4, 4), (5, 5, 5),
(6, 6, 6)]]
  if rank == 1:
    ll = [[(7, 7, 7)], [(8, 8, 8), (9, 9, 9)], [(10, 10, 10), (11, 11, 11),
(12, 12, 12)], []]
  print exchange(ll, comm)
