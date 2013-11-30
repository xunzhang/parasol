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
   
    def __stripoff(self, string, suffix, l, r):
        return string.strip(suffix).strip(l).strip(r)

    def __group_op_push(self, sz1, sz2):
	paralg.paralg_batch_write(self, (lambda i : list(self.p[i, :])), (lambda index_st : 'p[' + index_st + ',:]_' + str(self.nodeid / self.b)), sz1)
	paralh.paralg_batch_write(self, (lambda j : list(self.q[:, j])), (lambda index_st : 'q[:,' + index_st + ']_' + str(self.nodeid % self.b)), sz2)
    
    def __group_op_pull(self, sz1, sz2):
        def foo(i, val):
	    self.p[i, :] = val
	def goo(j, val):
	    self.q[:, j] = val
        paralg.paralg_batch_read(self, foo, (lambda index_st : 'p[' + index_st + ',:]_' + str(self.nodeid / self.b)), (lambda val : self.__stripoff(val, str(self.nodeid / self.b), 'p[', ',:]_')), sz1, True)
	paralg.paralg_batch_read(self, goo, (lambda index_st : 'q[:,' + index_st + ']_' + str(self.nodeid % self.b)), (lambda val : self.__stripoff(val, str(self.nodeid % self.b), 'q[:,', ']_')), sz2, True)

    def __group_op_update(self, sz1, sz2):
        paralg.paralg_batch_inc_nodelta(self, (lambda i : self.p[i, :]), (lambda index_st : 'p[' + index_st + ',:]_' + str(self.nodeid / self.b)), sz1)
	paralg.paralg_batch_inc_nodelta(self, (lambda j : self.q[:, j]), (lambda index_st : 'q[:,' + index_st + ']_' + str(self.nodeid % self.b)), sz2)

    def __mf_kernel(self):
        import time
	pl_sz = self.p.shape[0]
        ql_sz = self.q.shape[1]
        old_p = np.random.rand(pl_sz, self.k)
        old_q = np.random.rand(self.k, ql_sz)
        print 'data size: ', len(self.graph), '[', self.nodeid, ']'
	print 'decomp info: ', self.a, self.b, '[', self.nodeid, ']'
	# main phase
	for it in xrange(self.rounds):
            print 'round: ', it, '[', self.nodeid, ']'
	    s = time.time()
	    self.__group_op_pull(pl_sz, ql_sz)
	    #for index in xrange(pl_sz):
	    #    key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	    #    self.p[index, :] = paralg.paralg_read(self, key)
	    #for index in xrange(ql_sz):
	    #    key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
	    #    self.q[:, index] = paralg.paralg_read(self, key)
	    
	    f = time.time()
	    print 'local pull time is: ', f - s, '[', self.nodeid, ']'

	    # shuffle data
            random.shuffle(self.graph)
            
	    # record
	    for i in xrange(old_p.shape[0]):
	        for j in xrange(old_p.shape[1]):
	    	    old_p[i][j] = self.p[i][j]
	    for i in xrange(old_q.shape[0]):
	        for j in xrange(old_q.shape[1]):
	    	    old_q[i][j] = self.q[i][j]

	    s = time.time()
	    # learning
            for i, j, v in self.graph:
	        eij = v - np.dot(self.p[i, :], self.q[:, j])
	        tmpp = self.alpha * (2 * eij * self.q[:, j] - self.beta * self.p[i, :])
		tmpq = self.alpha * (2 * eij * self.p[i, :] - self.beta * self.q[:, j])
		self.p[i, :] += tmpp
		self.q[:, j] += tmpq
	    f = time.time()
	    print 'local learning time is: ', f - s, '[', self.nodeid, ']'
	   
	    s = time.time()
	    # push delta
	    for index in xrange(pl_sz):
	        key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	        paralg.paralg_inc(self, key, (self.p[index, :] - old_p[index, :]) / self.b)
	    for index in xrange(ql_sz):
	        key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
                paralg.paralg_inc(self, key, (self.q[:, index] - old_q[: ,index]) / self.a)
            f = time.time()
            print 'inc time is: ', f - s, '[', self.nodeid, ']'

	    # commit clock
	    paralg.iter_done(self)
    
        # last pull p, may not calc on this procs, but can be calced on others
        paralg.sync(self)
        start = clock()
	for index in xrange(pl_sz):
	    key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	    self.p[index, :] = paralg.paralg_read(self, key)
	for index in xrange(ql_sz):
	    key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
	    self.q[:, index] = paralg.paralg_read(self, key)
	paralg.sync(self)
        self.q = self.q.transpose()
        
    def __matrix_factorization(self):
	import time
	dimx, dimy = paralg.get_dataset_dim(self)
	# init self.p and self.q
        self.p = np.random.rand(dimx, self.k)
        self.q = np.random.rand(self.k, dimy)
        print 'before init push'
        # init push dimx and dimy
	s = time.time()
	for index in xrange(dimx):
	    key = 'p[' + str(index) + ',:]_' + str(self.nodeid / self.b)
	    paralg.paralg_write(self, key, self.p[index, :])
	for index in xrange(dimy):
	    key = 'q[:,' + str(index) + ']_' + str(self.nodeid % self.b)
	    paralg.paralg_write(self, key, self.q[:, index])
        print 'finish init push'
	f = time.time()
	print 'init time', f - s, '[', self.nodeid, ']'
	paralg.sync(self)
	# kernel mf solver
        self.__mf_kernel()
	paralg.sync(self)

    def solve(self):
        import time
        from parasol.utils.lineparser import parser_b
        #from parasol.utils.lineparser import parser_ussrt
	paralg.loadinput(self, self.filename, parser_b(','), 'fsmap') 
	#paralg.loadinput(self, self.filename, parser_ussrt, 'fsmap') 
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
	    tmp = np.dot(self.p[i, :], q[:, j])
	    esum += (v - np.dot(self.p[i, :], q[:, j])) ** 2
            #esum += (v - tricky(tmp)) ** 2
        return esum
    
    def calc_rmse(self):
        import math
        esum = self.__calc_esum()
	paralg.sync(self)
        esum = self.comm.allreduce(esum, op = MPI.SUM)
	cnt = self.comm.allreduce(len(self.graph), op = MPI.SUM)
        return math.sqrt(esum / cnt)
         
    def write_mf_result(self):
        if self.nodeid % self.b == 0:
            outputvec(self.outp, self.rmap, self.p, self.k, self.comm, self.suffix)
        if self.nodeid < self.b:
            outputvec(self.outq, self.cmap, self.q, self.k, self.comm, self.suffix)
