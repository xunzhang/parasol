#! /usr/bin/python

import os
from mpi4py import MPI

def expand_dir_rec(dn):
    import glob
    flst = []
    if not dn.endswith('/'):
        dn += '/'
    items = glob.glob(dn + '*')
    for it in items:
        if os.path.isfile(it):
            flst.append(it)
        else:
            flst += expand_dir_rec(it)
    return flst
  
def expand(fns):
    import glob
    import types
    flst = [] 
    if isinstance(fns, list):
        for it in fns:
            if os.path.isfile(it):
                flst.append(it)
            elif os.path.isdir(it):
                flst += expand_dir_rec(it)
	    else:
	        flst += glob.glob(it)
        return flst
    elif os.path.isfile(fns):
        return [fns]
    elif os.path.isdir(fns):
        return expand_dir_rec(fns)
    else:
        return glob.glob(fns)

def sendrecv(sbuf, sto, stag, rfrom, rtag, comm):
    req = comm.isend(sbuf, dest = sto, tag = stag)
    rbuf = comm.recv(source = rfrom, tag = rtag)
    req.wait()
    return rbuf

def bcastring(sendbuf, func, comm):
    rk = comm.Get_rank()
    sz = comm.Get_size()
    func(sendbuf)
    if sz == 1: return
    for i in xrange(1, sz):
        f = (rk + i) % sz
	t = (rk + sz - i) % sz
	rbuf = sendrecv(sendbuf, t, 2013, f, 2013, comm)
	func(rbuf) 
