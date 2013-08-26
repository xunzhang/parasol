#! /usr/bin/python
#
# wuhong<wuhong@douban.com>
#
# matrix factorization using gradient descent
#
import time
import sys
sys.path.append('../prototype/')
sys.path.append('/home/xunzhang/xunzhang/Data/book_interest/')
sys.path.append('../../xz_utils/')
import numpy as np
from parallel import *
from clt import kv
from scipy import sparse
from hash_ring import HashRing

'''
@input:
    r    : matrix to be factorized, u x i
    dimu : dim0 of r
    dimi : dim1 of r
    p    : matrix u x k reflect user to features
    q    : matrix i x k reflect item to features
    k    : number of latent features
    alpha: learning rate
    beta : regularization para
    steps: maximum number of steps to perform opt
    conv : convergence bound
@output:
    matrix p and q
'''
def mf_kernel(r, p, q, k, rank, b, alpha = 0.0002, beta = 0.02, steps = 200, conv = 0.0001):
    import random
    q = q.transpose()
    data_container = zip(r.row, r.col, r.data)
    random.shuffle(data_container)
    for it in xrange(steps):
        for i, j, v in data_container:
            spkey = 'p[' + str(i) + ',:]_' + str(rank / b)
            server1 = ring.get_node(spkey)
            p[i, :] = kvm[server1].pull(spkey)
            sqkey = 'q[:,' + str(j) + ']_' + str(rank % b)
            server2 = ring.get_node(sqkey)
            q[:,j] = kvm[server2].pull(sqkey) 
            eij = v - np.dot(p[i, :], q[:, j])
            deltap = []
            deltaq = []
            for ki in xrange(k):
                deltap.append(alpha * (2 * eij * q[ki][j] - beta * p[i][ki]))
                deltaq.append(alpha * (2 * eij * p[i][ki] - beta * q[ki][j]))
            # only need to set p[i, :] and q[:, j]
            kvm[server1].update(spkey, deltap)
            kvm[server2].update(sqkey, deltaq)
            #p[i,:] = kvm[server1].pull(spkey)
            #q[:,j] = kvm[server2].pull(sqkey)
            # check if convergent
            esum = 0
            for i, j, v in data_container:
                esum += (v - np.dot(p[i, :], q[:, j])) ** 2
                for ki in xrange(k):
                    esum += (beta / 2) * (p[i][ki] ** 2 + q[ki][j] ** 2)
            #esum = comm.allreduce(esum, op = MPI.SUM)
            if rank == 1:
                print it
                print 'esum is', esum, 'rank', rank
            #if esum < conv:
            #    break
    comm.barrier()
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
    return p, q.transpose()

'''prepare for mf, init p and q, use mf_kernel to solve'''
def matrix_factorization(r, u, i, k, rank, b):
    #u = r.shape[0]
    #i = r.shape[1]
    # init p & q
    p = np.random.rand(u, k)
    q = np.random.rand(i, k)
    for index in xrange(u):
      key = 'p[' + str(index) + ',:]_' + str(rank / b)
      server = ring.get_node(key)
      kvm[server].push(key, list(p[index, :]))
    for index in xrange(i):
      key = 'q[:,' + str(index) + ']_' + str(rank % b)
      server = ring.get_node(key)
      kvm[server].push(key, list(q[index, :]))
    comm.barrier()
    # kernel mf solver
    mf_kernel(r, p, q, k, rank, b)
    return p, q

servers = [0]
#servers = [0, 1]
#servers = [0, 1, 2]
ring = HashRing(servers)

if __name__ == '__main__':
    from mpi4py import MPI
    from crtblkmtx import ge_blkmtx 
    from writer import output
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    a, b = npfact2D(4)
    print a, b
    k = 20
    filename = '/home/xunzhang/xunzhang/Data/book_interest/testfile'
    outputfn = '/home/xunzhang/xunzhang/result.csv'
    rmap, cmap, mtx = ge_blkmtx(filename, comm)
    comm.barrier()
    kvm = [kv('localhost', '7907')]
    #kvm = [kv('localhost', '7907'), kv('localhost', '8907')]
    #kvm = [kv('localhost', '7907'), kv('localhost', '8907'), kv('localhost', '9907')]
    comm.barrier()
    print 'init done', rank
    p, q = matrix_factorization(mtx, len(rmap), len(cmap), k, rank, b)
    print 'calc done', rank
    bmtx = np.dot(p, q.T)
    output(outputfn, rmap, cmap, bmtx, comm) 
