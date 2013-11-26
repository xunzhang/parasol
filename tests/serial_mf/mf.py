#!/usr/bin/python
import numpy
from scipy.sparse import coo_matrix

def matrix_factorization(N, M, R, P, Q, K, steps=6, alpha=0.002, beta=0.02):
    Q = Q.T
    delta_p = numpy.random.rand(N, K)
    delta_q = numpy.random.rand(K, M)
    for step in xrange(steps):
        if step < 10:
	    alpha = 0.01
	else:
	    alpha = 0.002
	for i in xrange(N):
	    for j in xrange(K):
	        delta_p[i][j] = 0.
	for i in xrange(K):
	    for j in xrange(M):
	        delta_q[i][j] = 0.
	
        for i, j, v in R:
	    eij = v - numpy.dot(P[i,:], Q[:,j])
	    for k in xrange(K):
		#P[i][k] += alpha * (2 * eij * Q[k][j] - beta * P[i][k])
		#Q[k][j] += alpha * (2 * eij * P[i][k] - beta * Q[k][j])
		delta_p[i][k] = alpha * (2 * eij * Q[k][j] - beta * P[i][k])
		P[i][k] += delta_p[i][k]
		delta_q[k][j] = alpha * (2 * eij * P[i][k] - beta * Q[k][j])
		Q[k][j] += delta_q[k][j]
    return P, Q.T

def rmse(R, W, H):
    import math
    esum = 0.
    cnt = 0
    for i, j, v in R:
        esum += (numpy.dot(W[i, :], H[j, :]) - v) ** 2
	cnt += 1
    return math.sqrt(esum / cnt)

def load(f):
    i = []
    j = []
    v = []
    for line in f:
        arr = line.split(',')
	i.append(arr[0])
	j.append(arr[1])
	v.append(float(arr[2]))
    return i, j, v

def index_mapping(i, j):
    rdct = {}
    rev_rdct = {}
    cdct = {}
    rev_cdct = {}
    newi = []
    newj = []
    rcnt = 0
    ccnt = 0
    for item in i:
        if rev_rdct.get(item) == None:
	    rdct[rcnt] = item
	    rev_rdct[item] = rcnt
	    rcnt += 1
	newi.append(rev_rdct[item])
    for item in j:
        if rev_cdct.get(item) == None:
	    cdct[ccnt] = item
	    rev_cdct[item] = ccnt
	    ccnt += 1
	newj.append(rev_cdct[item])
    return newi, newj, rdct, cdct


if __name__ == "__main__":
    #i = [0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4, 4, 4]
    #j = [0, 1, 3, 0, 3, 0, 1, 3, 0, 3, 1, 2, 3]
    #v = [5, 3, 1, 4, 1, 1, 1, 5, 1, 4, 1, 5, 4]
    #f = file('/home/xunzhang/xunzhang/Data/mf/001.csv')
    f = file('/home/xunzhang/xunzhang/Data/mf/movielen10k')
    #f = file('/home/xunzhang/xunzhang/Data/interest/training.csv')
    #f = file('/home/xunzhang/xunzhang/Data/mf/002.csv')
    i, j, v = load(f)
    i, j, rmap, cmap = index_mapping(i, j)
    R = zip(i, j, v)
    N = len(rmap) #max(i) + 1
    M = len(cmap) #max(j) + 1
    print N
    print M
    K = 30
    P = numpy.random.rand(N,K)
    Q = numpy.random.rand(M,K)
    #P = numpy.array([[0.46500647, 0.10950494],
    #[0.47867466, 0.49807307],
    #[0.33376554, 0.24195584]])
    #P = numpy.array([[0.9739632, 0.37057525],
    #[0.64395383, 0.30542925]])
    #P = numpy.array([[0.46500647, 0.10950494],
    #[0.47867466, 0.49807307],
    #[0.33376554, 0.24195584],
    #[0.9739632, 0.37057525],
    #[0.64395383, 0.30542925]])
    #Q = numpy.array([[0.96802557, 0.22467435, 0.49266987, 0.18327164],
    #[0.81976892, 0.57031032, 0.22665017, 0.23747223]])
    #Q = Q.T


    nP, nQ = matrix_factorization(N, M, R, P, Q, K)
    print rmse(R, nP, nQ)
