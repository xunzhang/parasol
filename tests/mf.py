#! /usr/bin/python
#
# wuhong<wuhong@douban.com>
#
# matrix factorization using gradient descent
#

try:
    import numpy as np
except:
    print 'Please install numpy first.'
    exit(0)
from pykv import pykv


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
def mf_kernel(r, dimu, dimi, p, q, k, alpha = 0.0002, beta = 0.02, steps = 50000, conv = 0.0001):
    q = q.transpose()
    for it in xrange(steps):
        for i in xrange(dimu):
            for j in xrange(dimi):
                # for every rij
                if r[i][j] != 0:
                    p = kvstore.get('p')
                    q = kvstore.get('q')
                    eij = r[i][j] - np.dot(p[i, :], q[:, j])
                    for ki in xrange(k):
                        p[i][ki] += alpha * (2 * eij * q[ki][j] - beta * p[i][ki])
                        q[ki][j] += alpha * (2 * eij * p[i][ki] - beta * q[ki][j])
                    kvstore.set('p', p)
                    kvstore.set('q', q)
        # check if convergent
        esum = 0
        for i in xrange(dimu):
            for j in xrange(dimi):
                if r[i][j] != 0:
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
    
    kvstore.set_multi({'p' : p, 'q' : q.transpose()})
    #kvstore.get_multi(['p', 'q'])
    # kernel mf solver
    mf_kernel(r, u, i, p, q, k)
    kvstore.finalize()
    
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
    kvstore = pykv()
    p, q = matrix_factorization(mtx, k)
    print 'p is: '
    print p
    print 'q is: '
    print q
    print np.dot(p, q.T)
