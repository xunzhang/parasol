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
def mf_kernel(r, dimu, dimi, p, q, k, alpha = 0.0002, beta = 0.02, steps = 100, conv = 0.0001):
    q = q.transpose()
    for it in xrange(steps):
        for i in xrange(dimu):
            for j in xrange(dimi):
                # for every rij
                if r[i][j] != 0:
                    # run at each procs, store locally
                    # only need to get p[i, :] and q[:, j]
                    print 'before'
                    print p[i, :]
                    print q[:, j]
                    spkey = 'p[' + str(i) + ',:]'
                    p[i, :] = kvm.pull(spkey)
                    sqkey = 'q[:,' + str(j) + ']'
                    q[:,j] = kvm.pull(sqkey) 
                    print 'after0'
                    print p[i, :]
                    print q[:, j]
                    eij = r[i][j] - np.dot(p[i, :], q[:, j])
                    print r[i][j]
                    print eij
                    deltap = []
                    deltaq = []
                    for ki in xrange(k):
                        deltap.append(alpha * (2 * eij * q[ki][j] - beta * p[i][ki]))
                        deltaq.append(alpha * (2 * eij * p[i][ki] - beta * q[ki][j]))
                        #p[i, :] += alpha * (2 * eij * q[ki][j] - beta * p[i][ki])
                        #q[:, j] += alpha * (2 * eij * p[i][ki] - beta * q[ki][j])
                    # only need to set p[i, :] and q[:, j]
                    print 'after'
                    print deltap
                    print deltaq
                    kvm.update(spkey, deltap)
                    kvm.update(sqkey, deltaq)
                    p[i,:] = kvm.pull(spkey)
                    q[:,j] = kvm.pull(sqkey)
                    #print 'deltap', deltap
                    #time.sleep(0.1)
                    #kvm.push(spkey, p[i, :])
                    #kvm.push(sqkey, q[:, j])
        # check if convergent
        print 'sstart'
        print p
        print q
        print 'eend'
        esum = 0
        for i in xrange(dimu):
            for j in xrange(dimi):
                if r[i][j] != 0:
                    #spkey = 'p[' + str(i) + ',:]'
                    #sqkey = 'q[:,' + str(j) + ']'
                    #p[i, :] = kvm.pull(spkey)
                    #q[:, j] = kvm.pull(sqkey)
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
    kvm.push('p[0,:]', [0.65918662, 0.65221886])
    kvm.push('p[1,:]', [0.72454662, 0.5592554])
    kvm.push('p[2,:]', [0.0513879, 0.53047435])
    kvm.push('p[3,:]', [0.80869252, 0.37988651])
    kvm.push('p[4,:]', [0.4690092, 0.08531032])
    kvm.push('q[:,0]',[0.40330888, 0.61473888])
    kvm.push('q[:,1]',[0.35554828, 0.95415641])
    kvm.push('q[:,2]',[0.52891785, 0.46115004])
    kvm.push('q[:,3]',[0.21381893, 0.34623335])
    kvm.push('q[:,4]',[0.1612169, 0.76223091])
    kvm.push('q[:,5]',[0.77667506, 0.71933245])
    kvm.push('q[:,6]',[0.42349893, 0.73313934])
    #for index in xrange(u):
    #  key = 'p[' + str(index) + ',:]'
    #  kvm.push(key, list(p[index, :]))
    #for index in xrange(i):
    #  key = 'q[:,' + str(index) + ']'
    #  kvm.push(key, list(q[index, :]))
    #kvm.push_multi({'p[0,:]' : list(p[0, :]), 'p[1,:]' : list(p[1, :]), 'p[2,:]' : list(p[2, :]), 'p[3,:]' : list(p[3, :]), 'p[4,:]' : list(p[4, :])})
    #kvm.push_multi({'q[:,0]' : list(q[0, :]), 'q[:,1]' : list(q[1, :]), 'q[:,2]' : list(q[2, :]), 'q[:,3]' : list(q[3, :])})
    # kernel mf solver
    mf_kernel(r, u, i, p, q, k)
    print np.dot(p, q.transpose()) 
    return p, q

if __name__ == '__main__':
    k = 20
    filename = '/home/xunzhang/xunzhang/Data/book_interest/testfile'
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
    print mtx
    kvm = kv('localhost', 7900)
    print 'init done'
    k = 2
    ii = np.array([1, 1, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 3, 0, 1, 2, 3, 4, 4, 4])
    jj = np.array([5, 4, 0, 4, 1, 2, 3, 4, 3, 4, 0, 1, 2, 6, 6, 6, 6, 2, 4, 6])
    vv = np.array([2, 1, 1, 3, 1, 4, 3, 1, 2, 3, 3, 2, 4, 1, 2, 3, 4, 5, 7, 5])
    mtx = np.array(sparse.coo_matrix((vv, (ii, jj))).todense())
    p, q = matrix_factorization(mtx, k)
    # varify: (3, 193) 4.15350877193
    print mtx[56][65]
    print np.dot(p, q.T)[56][65]
    # 4.15350877193
    print mtx[17][46]
    print np.dot(p, q.T)[17][46]
