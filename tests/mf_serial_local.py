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
import numpy as np
#from mpi4py import MPI
#from pykv import pykv
from clt import kv
import pparse as par
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
def mf_kernel(r, dimu, dimi, p, q, k, alpha = 0.0002, beta = 0.02, steps = 3000, conv = 0.0001):
    q = q.transpose()
    for it in xrange(steps):
        for i in xrange(dimu):
            for j in xrange(dimi):
                # for every rij
                if r[i][j] != 0:
                    # run at each procs, store locally
                    # only need to get p[i, :] and q[:, j]
                    #print 'before'
                    #print p[i, :]
                    #print q[:, j]
                    eij = r[i][j] - np.dot(p[i, :], q[:, j])
                    #print r[i][j]
                    #print eij
                    deltap = []
                    deltaq = []
                    for ki in xrange(k):
                        deltap.append(alpha * (2 * eij * q[ki][j] - beta * p[i][ki]))
                        deltaq.append(alpha * (2 * eij * p[i][ki] - beta * q[ki][j]))
                    p[i, :] += deltap
                    q[:, j] += deltaq
                    #print 'after'
                    #print deltap
                    #print deltaq
                    #print p[i, :]
                    #print q[:, j]
                    # only need to set p[i, :] and q[:, j]
        # check if convergent
        esum = 0
        for i in xrange(dimu):
            for j in xrange(dimi):
                if r[i][j] != 0:
                    esum += (r[i][j] - np.dot(p[i, :], q[:, j])) ** 2
                    for ki in xrange(k):
                        esum += (beta / 2) * (p[i][ki] ** 2 + q[ki][j] ** 2)
        print 'esum is', esum
        print it
        if esum < conv:
            break
    return p, q.transpose()

'''prepare for mf, init p and q, use mf_kernel to solve'''
def matrix_factorization(r, k):
    # get dims
    u = r.shape[0]
    i = r.shape[1]
    print u
    print i
    # init p & q
    p = np.random.rand(u, k)
    q = np.random.rand(i, k)
    #p = np.array([[0.65918662, 0.65221886], [0.72454662, 0.5592554], [0.0513879, 0.53047435], [0.80869252, 0.37988651], [0.4690092, 0.08531032]])
    #q = np.array([[0.40330888, 0.61473888], [0.35554828, 0.95415641], [0.52891785, 0.46115004], [0.21381893, 0.34623335], [0.1612169, 0.76223091], [0.77667506, 0.71933245], [0.42349893, 0.73313934]]) 
    #print p
    #print q
    # kernel mf solver
    mf_kernel(r, u, i, p, q, k)
    print np.dot(p, q.transpose()) 
    return p, q

if __name__ == '__main__':
    k = 20
    filename = '/home/xunzhang/xunzhang/Data/book_interest/001.csv'
    pavg = par.avg(filename, 'P')
    uid_lst, sid_lst, rating = par.init(filename, 'P', pavg)
    dx, dy, rating_matrix = par.create_matrix(uid_lst, sid_lst, rating)
    rating_mtx = par.gen_sparse(rating_matrix)
    # l = b.getrow(2).todense()
    # list(numpy.array(l)[0])
    print rating_mtx
    print type(rating_mtx)
    print rating_mtx.todense()
    print type(rating_mtx.todense())
    mtx = np.array(rating_mtx.todense())
    print 'init done'
    #k = 2
    #ii = np.array([1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 3, 0, 1, 2, 3, 4, 4, 4])
    #jj = np.array([5, 4, 0, 4, 1, 2, 3, 4, 3, 4, 0, 1, 2, 6, 6, 6, 6, 2, 4, 6])
    #vv = np.array([2, 1, 1, 3, 1, 4, 3, 1, 2, 3, 3, 2, 4, 1, 2, 3, 4, 5, 7, 5])
    #mtx = np.array(sparse.coo_matrix((vv, (ii, jj))).todense())
    print mtx
    p, q = matrix_factorization(mtx, k)
    # varify: (3, 193) 4.15350877193
    print mtx[506][11407]
    print np.dot(p, q.T)[56][65]
    # 4.15350877193
    print mtx[17][46]
    print np.dot(p, q.T)[17][46]
