#! /usr/bin/python
#
# wuhong<wuhong@douban.com>
#
# matrix factorization using gradient descent
#

import sys
sys.path.append('../prototype/')
import numpy as np
#from mpi4py import MPI
from pykv import pykv
from clt import kv

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
def mf_kernel(r, dimu, dimi, p, q, k, alpha = 0.0002, beta = 0.02, steps = 1000, conv = 0.0001):
    q = q.transpose()
    for it in xrange(steps):
        for i in xrange(dimu):
            for j in xrange(dimi):
                # for every rij
                if r[i][j] != 0:
                    # run at each procs, store locally
                    # only need to get p[i, :] and q[:, j]
                    spkey = 'p[' + str(i) + ',:]'
                    p[i, :] = kvm.pull(spkey)
                    sqkey = 'q[:,' + str(j) + ']'
                    q[:,j] = kvm.pull(sqkey) 
                    eij = r[i][j] - np.dot(p[i, :], q[:, j])
                    for ki in xrange(k):
                        p[i][ki] += alpha * (2 * eij * q[ki][j] - beta * p[i][ki])
                        q[ki][j] += alpha * (2 * eij * p[i][ki] - beta * q[ki][j])
                    # only need to set p[i, :] and q[:, j]
                    kvm.push(spkey, list(p[i, :]))
                    kvm.push(sqkey, list(q[:, j]))
        # check if convergent
        esum = 0
        for i in xrange(dimu):
            for j in xrange(dimi):
                if r[i][j] != 0:
                    spkey = 'p[' + str(i) + ',:]'
                    sqkey = 'q[:,' + str(j) + ']'
                    p[i, :] = kvm.pull(spkey)
                    q[:, j] = kvm.pull(sqkey)
                    esum += (r[i][j] - np.dot(p[i, :], q[:, j])) ** 2
                    for ki in xrange(k):
                        esum += (beta / 2) * (p[i][ki] ** 2 + q[ki][j] ** 2)
        if esum < conv:
            break
    return p, q.transpose()

'''prepare for mf, init p and q, use mf_kernel to solve'''
def matrix_factorization(r, k):
    # get dims
    u = r.shape[0]
    i = r.shape[1]
    # init p & q
    p = np.random.rand(u, k)
    q = np.random.rand(i, k)
    print p 
    print q
    kvm.push_multi({'p[0,:]' : list(p[0, :]), 'p[1,:]' : list(p[1, :]), 'p[2,:]' : list(p[2, :]), 'p[3,:]' : list(p[3, :]), 'p[4,:]' : list(p[4, :])})
    kvm.push_multi({'q[:,0]' : list(q[0, :]), 'q[:,1]' : list(q[1, :]), 'q[:,2]' : list(q[2, :]), 'q[:,3]' : list(q[3, :])})
    
    # kernel mf solver
    mf_kernel(r, u, i, p, q, k)
    
    return p, q

if __name__ == '__main__':
    k = 2
    mtx = [
         [5, 3, 0, 1],
         [4, 0, 0, 1],
         [1, 1, 0, 5],
         [1, 0, 0, 4],
         [0, 1, 5, 4],
        ]
    mtx = np.array(mtx)
    kvm = kv('localhost', 7900)
    p, q = matrix_factorization(mtx, k)
    print 'p is: '
    print p
    print 'q is: '
    print q
    print np.dot(p, q.T)
