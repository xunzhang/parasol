#! /usr/bin/python
#
# Write (i, j, v) results to output file
#

def writelines(rmap, cmap, bmtx, dim1, comm):
  import time
  from mpi4py import MPI
  rank = comm.Get_rank()
  comm.barrier()
  outlst = []
  fntmp = ''
  if rank == 0:
    fntmp = '/tmp/' + str(time.time()) + '_rank_'
  fntmp = comm.bcast(fntmp, root = 0) 
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
  #for i in xrange(dim0):
  #  for j in xrange(dim1):
  #    content = ','.join([str(rmap[i]), str(cmap[j]), str(bmtx[i][j])])
  #    content = content + '\n'
  #    f.write(content)
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
  cmd0 = 'rm -rf ' + outdir
  cmd1 = 'mkdir ' + outdir 
  cmd2 = 'mv ' + fntmp + '* ' + outdir
  rank = comm.Get_rank()
  if rank == 0:
    print cmd1
    print cmd2
    os.system(cmd1)
    os.system(cmd2)
  
def output(outfile, rmap, cmap, bmtx, dim1, comm, mergeflag = False):
  fntmp = writelines(rmap, cmap, bmtx, dim1, comm)
  if mergeflag:
    mergefiles(fntmp, outfile)
  else:
    packfs(fntmp, outfile, comm)
