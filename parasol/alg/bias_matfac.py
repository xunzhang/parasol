# blocked matrix factorization using stochastic gradient 

import sys
import json
import numpy as np
from mpi4py import MPI
import random
from time import clock
from optparse import OptionParser
from parasol.decomp import npfact2d, npfactx
from parasol.writer.writer import outputvec    
from parasol.ps import paralg

#import pyximport
#pyximport.install(
#    setup_args = {
#    'include_dirs' : [
#    '/usr/include/python2.7']})
from parasol.c_ext.bias_matfac_kernel import clearn
    
class mf(paralg):

    def __init__(self, comm, hosts_dict_lst, nworker, k, input_filename, outp, outq, alpha = 0.002, beta = 0.02, rounds = 3, limit_s = 3):
        paralg.__init__(self, comm, hosts_dict_lst, nworker, rounds, limit_s)
        self.filename = input_filename
        self.outp = outp
        self.outq = outq
	self.k = k
        self.alpha = alpha
        self.beta = beta
        self.rounds = rounds
        self.nodeid = paralg.get_nodeid(self)
        # create folder
        paralg.crt_outfolder(self, self.outp)
        paralg.crt_outfolder(self, self.outq)
	paralg.sync(self)
    
    def init_parameter(self):
        self.p = None
	self.q = None
	self.usr_bias = None
	self.item_bias = None
	self.miu = 0.

    def accumulator(self, a, b):
        return a + b
    
    def __mf_kernel(self):
        import time
	pl_sz = self.p.shape[0]
        ql_sz = self.q.shape[1]
        old_p = np.random.rand(pl_sz, self.k)
        old_q = np.random.rand(self.k, ql_sz)
	old_ub = np.random.rand(pl_sz)
	old_ib = np.random.rand(ql_sz)
        print 'data size: ', len(self.graph), '[', self.nodeid, ']'
	print 'decomp info: ', self.a, self.b, '[', self.nodeid, ']'
	# main phase
	for it in xrange(self.rounds):
            print 'round: ', it, '[', self.nodeid, ']'
	    
	    #if it % 3:
	    s = time.time()
	    for index in xrange(pl_sz):
	        key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	        self.p[index, :] = paralg.paralg_read(self, key)
		self.usr_bias[index] = paralg.paralg_read(self, 'usr_bias_' + str(index)) 
	    for index in xrange(ql_sz):
	        key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
	        self.q[:, index] = paralg.paralg_read(self, key)
		self.item_bias[index] = paralg.paralg_read(self, 'item_bias_' + str(index))
            f = time.time()
	    #print 'local pull time is : ', f - s, '[', self.nodeid, ']'

	    # shuffle data
            random.shuffle(self.graph)
            
	    # record
	    for i in xrange(old_p.shape[0]):
	        for j in xrange(old_p.shape[1]):
		    old_p[i][j] = self.p[i][j]
            for i in xrange(len(old_ub)):
		old_ub[i] = self.usr_bias[i]
	    for i in xrange(old_q.shape[0]):
	        for j in xrange(old_q.shape[1]):
		    old_q[i][j] = self.q[i][j]
	    for i in xrange(len(old_ib)):
		old_ib[i] = self.item_bias[i]
	   
	    s = time.time()
	    # learning
            for i, j, v in self.graph:
	        #pr = self.miu + self.usr_bias[i] + self.item_bias[j] + np.dot(self.p[i, :], self.q[:, j])
	        #eij = v - pr
	        #tmpp = self.alpha * (2 * eij * self.q[:, j] - self.beta * self.p[i, :])
		#tmpq = self.alpha * (2 * eij * self.p[i, :] - self.beta * self.q[:, j])
		#tmpub = self.alpha * (2 * eij - self.beta * self.usr_bias[i])
		#tmpib = self.alpha * (2 * eij - self.beta * self.item_bias[j])
		tmpp, tmpq, tmpub, tmpib = clearn(v, self.alpha, self.beta, self.miu, self.usr_bias[i], self.item_bias[j], self.p[i, :], self.q[:, j])
		self.p[i, :] += tmpp
		self.q[:, j] += tmpq
		self.usr_bias[i] += tmpub
		self.item_bias[j] += tmpib
	    f = time.time()
	    #print 'local learning time is : ', f - s, '[', self.nodeid, ']'

	    # push delta
	    for index in xrange(pl_sz):
	        key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	        paralg.paralg_inc(self, key, (self.p[index, :] - old_p[index, :]) / self.b)
	        paralg.paralg_inc(self, 'usr_bias_' + str(index), (self.usr_bias[index] - old_ub[index]) / self.b)
	    for index in xrange(ql_sz):
	        key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
                paralg.paralg_inc(self, key, (self.q[:, index] - old_q[: ,index]) / self.a)
                paralg.paralg_inc(self, 'item_bias_' + str(index), (self.item_bias[index] - old_ib[index]) / self.a)
	    
	    # commit clock
	    paralg.iter_done(self)
    
        # last pull p, may not calc on this procs, but can be calced on others
        paralg.sync(self)
        start = clock()
	for index in xrange(pl_sz):
	    key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	    self.p[index, :] = paralg.paralg_read(self, key)
	    self.usr_bias[index] = paralg.paralg_read(self, 'usr_bias_' + str(index))
	for index in xrange(ql_sz):
	    key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
	    self.q[:, index] = paralg.paralg_read(self, key)
	    self.item_bias[index] = paralg.paralg_read(self, 'item_bias_' + str(index))
	paralg.sync(self)
        self.q = self.q.transpose()
        
    def __matrix_factorization(self):
	dimx, dimy = paralg.get_dataset_dim(self)
	# init self.p and self.q
	self.miu = paralg.get_global_miu(self)
        self.p = np.random.rand(dimx, self.k)
        self.q = np.random.rand(self.k, dimy)
	self.usr_bias = np.random.rand(dimx)
	self.item_bias = np.random.rand(dimy)
        print 'before init push'
        # init push dimx and dimy
	for index in xrange(dimx):
	    key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	    paralg.paralg_write(self, key, self.p[index, :])
	    paralg.paralg_write(self, 'usr_bias_' + str(index), self.usr_bias[index])
	for index in xrange(dimy):
	    key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
	    paralg.paralg_write(self, key, self.q[:, index])
	    paralg.paralg_write(self, 'item_bias_' + str(index), self.item_bias[index])

        print 'finish init push'
	paralg.sync(self)
	# kernel mf solver
        self.__mf_kernel()
	paralg.sync(self)

    def solve(self):
        import time
        from parasol.utils.lineparser import parser_b
        #from parasol.utils.lineparser import parser_ussrt
	paralg.loadinput(self, self.filename, parser_b(','), 'fsmap') 
        self.a, self.b = paralg.get_decomp(self)
	self.graph = paralg.get_graph(self)
	paralg.sync(self)
        s = time.time()
	self.__matrix_factorization()
        f = time.time()
	print 'nodeid ', self.nodeid, ' kernel time ', f - s

    def __calc_esum(self):
        def tricky(r):
	    import math
	    fm = math.floor(r)
	    if (r - fm) > 0.9:
	        return fm + 1
	    elif (r - fm) < 0.1:
	        return fm
	    else:
	        return r
        esum = 0.
        q = self.q.transpose()
        for i, j, v in self.graph:
	    tmp = self.miu + self.usr_bias[i] + self.item_bias[j] + np.dot(self.p[i, :], q[:, j])
	    esum += (v - tmp) ** 2
        return esum
    
    def calc_rmse(self):
        import math
        esum = self.__calc_esum()
	paralg.sync(self)
        esum = self.comm.allreduce(esum, op = MPI.SUM)
	cnt = self.comm.allreduce(len(self.graph), op = MPI.SUM)
        return math.sqrt(esum / cnt)
         
    def write_mf_result(self):
        p_fac_ubias = np.random.rand(self.p.shape[0], self.p.shape[1] + 1)
	q_fac_ibias = np.random.rand(self.q.shape[0], self.q.shape[1] + 1)
	for i in xrange(self.p.shape[0]):
	    p_fac_ubias[i] = np.append(self.p[i], self.usr_bias[i])
        for i in xrange(self.q.shape[0]):
	    q_fac_ibias[i] = np.append(self.q[i], self.item_bias[i])
	print self.miu
        if self.nodeid % self.b == 0:
            outputvec(self.outp, self.rmap, self.p, self.k, self.comm, self.suffix)
        if self.nodeid < self.b:
            outputvec(self.outq, self.cmap, self.q, self.k, self.comm, self.suffix)
