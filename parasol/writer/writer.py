#! /usr/bin/python
#
# Write (i, j, v) results to output file
#

def writelines(rmap, cmap, bmtx, comm):
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
  for i in xrange(bmtx.shape[0]):
    for j in xrange(bmtx.shape[1]):
      content = ','.join([str(rmap[i]), str(cmap[j]), str(bmtx[i][j])])
      content = content + '\n'
      f.write(content)
  comm.barrier()
  return fntmp

def mergefiles(fntmp, outfile):
  import glob
  fls = glob.glob(fntmp + '*')
  f = open(outfile, 'wb')
  for fl in fls:
    ff = open(fl, 'rb')
    for stf in ff:
      f.write(stf)

def packfs(fntmp, outdir):
  import os
  import glob
  fls = glob.glob(fntmp + '*')
  cmd1 = 'mkdir ' + outdir 
  os.system(cmd1)
  cmd2 = 'mv ' + fntmp + '* ' outdir
  os.system(cmd2)
  
def output(outfile, rmap, cmap, bmtx, comm, mergeflag = False):
  fntmp = writelines(rmap, cmap, bmtx, comm)
  if mergeflag:
    mergefiles(fntmp, outfile)
  else:
    packfs(fntmp, outfile)
