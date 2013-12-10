#! /usr/bin/python
# Matrix factorization with bias

import numpy as np
import pyximport
pyximport.install(
    setup_args={
    'include_dirs': [
    '/usr/include/python2.7']})

from learnkernel import learn 

class bmf():
    def __init__(self, k = 100, rounds = 50, alpha = 0.005, beta = 0.02, train_fn = '', validate_fn = '', pred_fn = '', output = ''):
        self.k = k
	self.rounds = rounds
	self.alpha = alpha
	self.beta = beta
	self.train_fn = train_fn
	self.pred_fn = pred_fn
	self.validate_fn = validate_fn
	self.output = output
        self.usr_dct = {}
	self.item_dct = {}
	self.rating_graph = {}
	self.rating_sz = 0
	self.miu = 0.
	self.p = None
	self.q = None
	self.usr_bias = None
	self.item_bias = None
	self.rmse = 0.
    
    def load(self):
        f = open(self.train_fn)
	for line in f:
	    uid, iid, rating = line.strip('\n').split(',')
            rating = float(rating)
	    if uid not in self.usr_dct:
	        self.usr_dct[uid] = len(self.usr_dct)
	    if iid not in self.item_dct:
	        self.item_dct[iid] = len(self.item_dct)
	    self.rating_graph.setdefault(self.usr_dct[uid], []).append((self.item_dct[iid], rating))
	    self.rating_sz += 1
            self.miu += rating
	self.miu /= self.rating_sz
	f.close()
    
    def cal_rmse(self):
        import math
        for u_indx, pair in self.rating_graph.iteritems():
	    for i_indx, rating in pair:
	        tmp = self.miu + self.usr_bias[u_indx] + self.item_bias[i_indx] + np.dot(self.p[u_indx, :], self.q[i_indx, :])
	        self.rmse += (rating - tmp) ** 2
        return math.sqrt(self.rmse / self.rating_sz)

    def predict_rating(self):
        f1 = open(self.pred_fn)
	f2 = open(self.output, 'w')
	for line in f1:
	    uid, iid = line.strip('\n').split(',')
	    u_indx = self.usr_dct[uid]
	    i_indx = self.item_dct[iid]
	    pred_rating = self.miu + self.usr_bias[u_indx] + self.item_bias[i_indx] + np.dot(self.p[u_indx, :], self.q[i_indx, :])
	    f2.write('%s,%s,%s\n' % (uid, iid, pred_rating))
	f1.close()
	f2.close()

    def r(self, i, j):
        return self.miu + self.usr_bias[i] + self.item_bias[j] + np.dot(self.p[i, :], self.q[j, :])

    def kernel(self):
        import time
	import ctypes
	#lib = ctypes.CDLL('./kernel.so')
	#lib.dot.argtypes = [ctypes.c_float, ctypes.c_float, ctypes.c_float, ctypes.POINTER(ctypes.c_float), ctypes.POINTER(ctypes.c_float), ctypes.c_int] 
	#lib.dot.restype = ctypes.c_float
	#c_farray_type = ctypes.POINTER(ctypes.c_float)
	# init parameters
	self.p = np.random.rand(len(self.usr_dct), self.k)
        self.q = np.random.rand(len(self.item_dct), self.k)
	self.usr_bias = np.random.rand(len(self.usr_dct))
	self.item_bias = np.random.rand(len(self.item_dct))
	# learning
	for rd in xrange(self.rounds):
	    start = time.time()
	    for u_indx, pair in self.rating_graph.iteritems():
	        for i_indx, rating in pair:
		    #tmp_p = self.p[u_indx, :].astype(np.float32)
		    #data_tmp_p = tmp_p.ctypes.data_as(c_farray_type)
		    #tmp_q = self.q[i_indx, :].astype(np.float32)
		    #data_tmp_q = tmp_q.ctypes.data_as(c_farray_type)
		    #tmp = lib.dot(ctypes.c_float(self.miu), ctypes.c_float(self.usr_bias[u_indx]), ctypes.c_float(self.item_bias[i_indx]), data_tmp_p, data_tmp_q, ctypes.c_int(self.k))
		    #tmp = self.miu + self.usr_bias[u_indx] + self.item_bias[i_indx] + np.dot(self.p[u_indx, :], self.q[i_indx, :])
		    #tmp = estimate(self.miu, self.usr_bias[u_indx], self.item_bias[i_indx], self.p[u_indx, :], self.q[i_indx, :])
		    #e = rating - tmp 
		    # learning delta
		    delta_p, delta_q, delta_ubias, delta_ibias = learn(rating, self.alpha, self.beta, self.miu, self.usr_bias[u_indx], self.item_bias[i_indx], self.p[u_indx, :], self.q[i_indx, :])
		    #delta_p = self.alpha * (2 * e * self.q[i_indx, :] - self.beta * self.p[u_indx, :])
		    #delta_q = self.alpha * (2 * e * self.p[u_indx, :] - self.beta * self.q[i_indx, :])
		    #delta_ubias = self.alpha * (2 * e - self.beta * self.usr_bias[u_indx])
		    #delta_ibias = self.alpha * (2 * e - self.beta * self.item_bias[i_indx])
		    #print delta_ubias
		    #print delta_ibias
		    # update with delata
		    self.p[u_indx, :] += delta_p
		    self.q[i_indx, :] += delta_q
                    self.usr_bias[u_indx] += delta_ubias
		    self.item_bias[i_indx] += delta_ibias
            end = time.time() 
	    print 'iter time: ', end - start
            print 'iter'

    def learn(self):
        '''api'''
        self.load()
	self.kernel()

if __name__ == '__main__':
    bmf_solver = bmf(k = 80, rounds = 3, alpha = 0.005, beta = 0.02, train_fn = '/home/xunzhang/xunzhang/Data/mf/movielen1m', validate_fn = 'lol', pred_fn = '/home/xunzhang/xunzhang/Data/mf/pred.csv', output = 'pred_rating.csv')
    bmf_solver.learn()
    #bmf_solver.predict_rating()
    print bmf_solver.cal_rmse()
