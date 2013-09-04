#! /usr/bin/python
#
# wuhong<wuhong@douban.com>
#
# matrix factorization using gradient descent
#
import time
import sys
import numpy as np
from parasol.utils.parallel import *
from parasol.clt import kv
from scipy import sparse
from hash_ring import HashRing
from time import clock

def calc_loss(r, p, q, k, beta = 0.02):
    esum = 0
    for i, j, v in zip(r.row, r.col, r.data):
        esum += (v - np.dot(p[i, :], q[:, j])) ** 2
        for ki in xrange(k):
            esum += (beta / 2) * (p[i][ki] ** 2 + q[ki][j] ** 2)
    return esum

def mf_kernel(r, p, q, k, rank, b, alpha = 0.0002, beta = 0.02, rounds = 5, conv = 0.0001):
    import random
    #from itertools import izip
    pl_size = p.shape[0]
    ql_size = q.shape[0]
    q = q.transpose()
    #data_container = izip(r.row, r.col, r.data)
    data_container = zip(r.row, r.col, r.data)
    print 'data size is', len(data_container)
    #random.shuffle(data_container)
    for it in xrange(rounds):
        print 'round', it
        if it != 0:
            for index in xrange(pl_size):
                key = 'p[' + str(index) + ',:]_' + str(rank / b)
                server = ring.get_node(key)
                p[index, :] = kvm[server].pull(key)
            for index in xrange(ql_size):
                key = 'q[:,' + str(index) + ']_' + str(rank % b)
                server = ring.get_node(key)
                q[:, index] = kvm[server].pull(key)
        
        print 'after'
        start = clock()
        random.shuffle(data_container)
        end = clock()
        print 'shuffle time is', end - start
        
        for i, j, v in data_container:
            eij = v - np.dot(p[i, :], q[:, j])
            for ki in xrange(k):
                p[i][ki] += alpha * (2 * eij * q[ki][j] - beta * p[i][ki])
                q[ki][j] += alpha * (2 * eij * p[i][ki] - beta * q[ki][j])
        
        print 'local done'
        start = clock()
        print 'calc time is', start - end
         
        for index in xrange(pl_size):
            key = 'p[' + str(index) + ',:]_' + str(rank / b)
            server = ring.get_node(key)
            deltap = list(p[index, :] - kvm[server].pull(key))
            kvm[server].update(key, deltap)
        for index in xrange(ql_size):
            key = 'q[:,' + str(index) + ']_' + str(rank % b)
            server = ring.get_node(key)
            deltaq = list(q[:, index] - kvm[server].pull(key))
            kvm[server].update(key, deltaq)
        
        #comm.barrier()
        end = clock()
        print 'communication time is', end - start
         
    comm.barrier()
    start = clock()
    # last pull p, may not calc on this procs, but can be calced on other procs
    for i in xrange(p.shape[0]):
        spkey = 'p[' + str(i) + ',:]_' + str(rank / b)
        server1 = ring.get_node(spkey)
        p[i, :] = kvm[server1].pull(spkey)
    # last pull q
    for j in xrange(q.shape[1]):
        sqkey = 'q[:,' + str(j) + ']_' + str(rank % b)
        server2 = ring.get_node(sqkey) 
        q[:, j] = kvm[server2].pull(sqkey)
    comm.barrier()
    end = clock()
    print 'last pull time is', end - start
    return p, q.transpose()

'''prepare for mf, init p and q, use mf_kernel to solve'''
def matrix_factorization(r, u, i, k, rank, b):
    #u = r.shape[0]
    #i = r.shape[1]
    # init p & q
    p = np.random.rand(u, k)
    q = np.random.rand(i, k)
    print 'before init push'
    for index in xrange(u):
      key = 'p[' + str(index) + ',:]_' + str(rank / b)
      server = ring.get_node(key)
      kvm[server].push(key, list(p[index, :]))
    for index in xrange(i):
      key = 'q[:,' + str(index) + ']_' + str(rank % b)
      server = ring.get_node(key)
      kvm[server].push(key, list(q[index, :]))
    print 'finish init push'
    comm.barrier()
    # kernel mf solver
    mf_kernel(r, p, q, k, rank, b)
    return p, q

servers = [0]
#servers = [0, 1]
#servers = [0, 1, 2]
ring = HashRing(servers)

if __name__ == '__main__':
    import os
    from mpi4py import MPI
    from parasol.loader.crtblkmtx import ge_blkmtx 
    from parasol.writer.writer import outputvec
    from optparse import OptionParser
    import json
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    para_cfg = json.loads(open('../config/mf_cfg.json').read())
    a, b = npfact2D(para_cfg['n'])
    k = para_cfg['k']
    filename = para_cfg['input']
    outputfnp = para_cfg['outputp']
    outputfnq = para_cfg['outputq']
    rmap, cmap, mtx = ge_blkmtx(filename, comm)
    comm.barrier()
    kvm = [kv('localhost', '7907')]
    #kvm = [kv('localhost', '7907'), kv('localhost', '8907')]
    #kvm = [kv('localhost', '7907'), kv('localhost', '8907'), kv('localhost', '9907')]
    comm.barrier()
    print 'init done', rank
    p, q = matrix_factorization(mtx, len(rmap), len(cmap), k, rank, b)
    print 'calc done', rank
    esum = calc_loss(mtx, p, q.T, k)
    print esum
    comm.barrier()
    esum = comm.allreduce(esum, op = MPI.SUM)
    #bmtx = np.dot(p, q.T)
    #bmtx_it = mm_mult(p, q.T)
    #output(outputfn, rmap, cmap, bmtx_it, q.shape[0], comm) 
    if rank == 0:
        if not os.path.exists(outputfnp):
            os.system('mkdir ' + outputfnp)
        if not os.path.exists(outputfnq):
            os.system('mkdir ' + outputfnq) 
    comm.barrier()
    if rank % b == 0:
        outputvec(outputfnp, rmap, p, k, comm, '_mf')
    if rank < b:
        outputvec(outputfnq, cmap, q, k, comm, '_mf')
