#! /usr/bin/python
#
# Write (i, j, v) results to output file
#

def writelinesvec(place, maps, vec, k, comm, suffix = ''):
  import os
  import time
  import random
  rank = comm.Get_rank()
  outlst = []
  fntmp = ''
  fntmp = place + 'result_rank_'
  fn = fntmp + str(rank) + suffix
  f = open(fn, 'wb')
  content = ''
  for i in xrange(vec.shape[0]):
    idstr = str(maps[i]) + ':'
    line = [str(j) for j in vec[i, :]]
    valstr = '|'.join(line)
    content = idstr + valstr + '\n'
    f.write(content)
  return fntmp

def writelinesmtx(place, rmap, cmap, bmtx, dim1, comm):
  import time
  rank = comm.Get_rank()
  comm.barrier()
  outlst = []
  fntmp = ''
  #if rank == 0:
  fntmp = place + str(time.time()) + '_rank_'
  #fntmp = comm.bcast(fntmp, root = 0) 
  fn = fntmp + str(rank)
  f = open(fn, 'wb')
  content = '' 
  rowcnt = 0
  colcnt = 0
  for stf in bmtx:
    content = ','.join([str(rmap[rowcnt]), str(cmap[colcnt]), str(stf)])
    content = content + '\n'
    f.write(content)
    colcnt += 1
    if colcnt == dim1:
      rowcnt += 1
      colcnt = 0
  comm.barrier()
  return fntmp

def mergefiles(fntmp, outfile, comm):
  import glob
  fls = glob.glob(fntmp + '*')
  rank = comm.Get_rank()
  if rank == 0:
    f = open(outfile, 'wb')
    for fl in fls:
      ff = open(fl, 'rb')
      for stf in ff:
        f.write(stf)

def packfs(fntmp, outdir, comm):
  import os
  import glob
  fls = glob.glob(fntmp + '*')
  cmd0 = 'rm -ri ' + outdir
  cmd1 = 'mkdir ' + outdir 
  cmd2 = 'mv ' + fntmp + '* ' + outdir
  rank = comm.Get_rank()
  if rank == 0:
    print cmd1
    print cmd2
    os.system(cmd1)
    os.system(cmd2)

def outputvec(outplace, maps, vec, k, comm, suffix = '', mergeflag = False):
  fntmp = writelinesvec(outplace, maps, vec, k, comm, suffix)
  if mergeflag:
    mergefiles(fntmp, outplace, comm)
  #else:
  #  packfs(fntmp, outplace, comm)  
  
def outputmtx(outfile, rmap, cmap, bmtx, dim1, comm, mergeflag = False):
  fntmp = writelinesmtx(rmap, cmap, bmtx, dim1, comm)
  if mergeflag:
    mergefiles(fntmp, outfile, comm)
  else:
    packfs(fntmp, outfile, comm)
