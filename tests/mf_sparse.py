#! /usr/bin/python
#
# wuhong<wuhong@douban.com>
#
# matrix factorization using gradient descent
#

import sys
sys.path.append('../prototype/')
import numpy as np
#from pykv import pykv
from clt import kv
from scipy import sparse

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
def mf_kernel(r, p, q, k, alpha = 0.0002, beta = 0.02, steps = 5000, conv = 0.0001):
    q = q.transpose()
    for it in xrange(steps):
        for i, j, v in zip(r.row, r.col, r.data):
            eij = v - np.dot(p[i, :], q[:, j])
            for ki in xrange(k):
                p[i][ki] += alpha * (2 * eij * q[ki][j] - beta * p[i][ki])
                q[ki][j] += alpha * (2 * eij * p[i][ki] - beta * q[ki][j])
        # check if convergent
        esum = 0
        for i, j, v in zip(r.row, r.col, r.data):
            esum += (v - np.dot(p[i, :], q[:, j])) ** 2
            for ki in xrange(k):
                esum += (beta / 2) * (p[i][ki] ** 2 + q[ki][j] ** 2)
        print 'esum is ',esum
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
    print q.transpose() 
    # kernel mf solver
    mf_kernel(r, p, q, k)
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
    import numpy as np
    from scipy.sparse import coo_matrix
    row = np.array([0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4])
    col = np.array([0, 1, 3, 0, 3, 0, 1, 3, 0, 3, 1, 2, 3])
    data = np.array([5, 3, 1, 4, 1, 1, 1, 5, 1, 4, 1, 5, 4])
    mtx = coo_matrix((data, (row, col)))
    p, q = matrix_factorization(mtx, k)
    print 'p is: '
    print p
    print 'q is: '
    print q
    print np.dot(p, q.T)
