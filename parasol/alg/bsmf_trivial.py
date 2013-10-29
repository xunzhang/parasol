# blocked matrix factorization using stochastic gradient 

import sys
import json
import numpy as np
from mpi4py import MPI
import random
from time import clock
from optparse import OptionParser
from parasol.utils.parallel import npfact2D 
from parasol.writer.writer import outputvec    
#from parasol.loader.crtblkmtx import ge_blkmtx
from parasol.ps import paralg
    
class bsmf(paralg):

    def __init__(self, comm, hosts_dict_lst, nworker, k, input_filename, outp, outq, alpha = 0.002, beta = 0.02, rounds = 3, limit_s = 3):
        paralg.__init__(self, comm, hosts_dict_lst, nworker, rounds, limit_s)
        self.rank = self.comm.Get_rank()
        self.a, self.b = npfact2D(nworker)
        self.k = k
        self.filename = input_filename
        self.outp = outp
        self.outq = outq
        
        self.alpha = alpha
        self.beta = beta
        self.rounds = rounds
        
        # create folder
        paralg.crt_outfolder(self, self.outp)
        paralg.crt_outfolder(self, self.outq)
        self.comm.barrier()
    
    def __mf_kernel(self):#, alpha = 0.0002, beta = 0.02, rounds = 5):
	import copy
        pl_sz = self.p.shape[0]
        ql_sz = self.q.shape[1]
        data_container = zip(self.mtx.row, self.mtx.col, self.mtx.data)
        print 'data size is', len(data_container)
        delta_p = np.random.rand(pl_sz, self.k)
        delta_q = np.random.rand(self.k, ql_sz)
        for it in xrange(self.rounds):
            print 'round', it, self.comm.Get_rank()
            if it != 0:
                #self.__group_op_1(pl_sz, ql_sz, 'pull')
                #self.__pack_op_1(pl_sz, ql_sz, 'pull')
                #self.__new_group_op_1(pl_sz, ql_sz, 'pull')
		for index in xrange(pl_sz):
		    key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
		    #srv = self.ring.get_node(key)
		    self.p[index, :] = paralg.paralg_read(self, key)
		for index in xrange(ql_sz):
		    key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
		    #srv = self.ring.get_node(key)
		    self.q[:,index] = paralg.paralg_read(self, key)
            print 'after round pull'
	    #old_p = copy.deepcopy(self.p)
            #old_q = copy.deepcopy(self.q)
            #for i, j, v in data_container:
            #    for ki in xrange(self.k):
            #        old_p[i][ki] = self.p[i][ki]
            #        old_q[ki][j] = self.q[ki][j]
	    for i in xrange(delta_p.shape[0]):
	        for j in xrange(delta_p.shape[1]):
                    delta_p[i][j] = 0.
            for i in xrange(delta_q.shape[0]):
                for j in xrange(delta_q.shape[1]):
                    delta_q[i][j] = 0.

            start = clock()
            random.shuffle(data_container)
            end = clock()
            print 'shuffle time is', end - start
            
            for i, j, v in data_container:
                eij = v - np.dot(self.p[i, :], self.q[:, j])
                for ki in xrange(self.k):
                    delta_p[i][ki] = self.alpha * (2 * eij * self.q[ki][j] - self.beta * self.p[i][ki])
                    delta_q[ki][j] = self.alpha * (2 * eij * self.p[i][ki] - self.beta * self.q[ki][j])
		    self.p[i][ki] += delta_p[i][ki]
                    self.q[ki][j] += delta_q[ki][j]
                    #self.p[i][ki] += self.alpha * (2 * eij * self.q[ki][j] - self.beta * self.p[i][ki])
                    #self.q[ki][j] += self.alpha * (2 * eij * self.p[i][ki] - self.beta * self.q[ki][j])
            start = clock()
            print 'local calc time is', start - end
            
            #self.__new_group_op_2(pl_sz, ql_sz)
	    for index in xrange(pl_sz):
		key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
                srv = self.ring.get_node(key)
		#deltap = list(self.p[index, :] - old_p[index, :])
                #deltap = list(self.p[index, :] - pp[index, :])
                #deltap = list(self.p[index, :] - self.kvm[srv].pull(key))
                #paralg.paralg_inc(self, key, deltap)
		paralg.paralg_inc(self, key, delta_p[index, :])
	    for index in xrange(ql_sz):
		key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
                srv = self.ring.get_node(key)
		#deltaq = list(self.q[:, index] - old_q[:, index])
                #deltaq = list(self.q[:,index] - qq[:, index])
                #deltaq = list(self.q[:,index] - self.kvm[srv].pull(key))
                #paralg.paralg_inc(self, key, deltaq)
		paralg.paralg_inc(self, key, delta_q[:, index])
            end = clock()
            print 'communication time is', end - start
	    paralg.iter_done(self)

        # last pull p, may not calc on this procs, but can be calced on others
        self.comm.barrier()
        start = clock()
        #self.__group_op_1(pl_sz, ql_sz, 'pull')
        #self.__pack_op_1(pl_sz, ql_sz, 'pull')
        #self.__new_group_op_1(pl_sz, ql_sz, 'pull')
        for index in xrange(pl_sz):
	    key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
            srv = self.ring.get_node(key)
            self.p[index, :] = paralg.paralg_read(self, key)
	for index in xrange(ql_sz):
            key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
            srv = self.ring.get_node(key)
            self.q[:,index] = paralg.paralg_read(self, key)
        self.comm.barrier()
        end = clock()
        print 'last pull time is', end - start
        self.q = self.q.transpose()
        
    def __matrix_factorization(self):
        u = len(self.rmap)
        i = len(self.cmap)
        self.p = np.random.rand(u, self.k)
        self.q = np.random.rand(self.k, i)
        print 'u is', u
        print 'i is', i
        print 'before init push'
        #self.__group_op_1(u, i, 'push')
        #self.__pack_op_1(u, i, 'push')
        #self.__new_group_op_1(u, i, 'push')
        for index in xrange(u):
            key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
            srv = self.ring.get_node(key)
            paralg.paralg_write(self, key, list(self.p[index, :]))
        for index in xrange(i):
            key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
            srv = self.ring.get_node(key)
            paralg.paralg_write(self, key, list(self.q[:,index]))
        print 'finish init push'
        self.comm.barrier()

        # kernel mf solver
        self.__mf_kernel()

    def solve(self):
        from parasol.utils.lineparser import parser_ussrt
        #self.rmap, self.cmap, self.mtx = ge_blkmtx(self.filename, self.comm)
	paralg.loadinput(self, self.filename, parser_ussrt, 'fsmap')
        self.comm.barrier()
        self.__matrix_factorization()
        self.comm.barrier()
    
    def __calc_esum(self):
        esum = 0
        q = self.q.transpose()
        for i, j, v in zip(self.mtx.row, self.mtx.col, self.mtx.data):
            esum += (v - np.dot(self.p[i, :], q[:, j])) ** 2
            for ki in xrange(self.k):
                esum += (self.beta / 2) * (self.p[i][ki] ** 2 + q[ki][j] ** 2)
        return esum
    
    def calc_loss(self):
        esum = self.__calc_esum()
        self.comm.barrier()
        esum = self.comm.allreduce(esum, op = MPI.SUM)
        return esum
         
    def write_bsmf_result(self):
        if self.rank % self.b == 0:
            outputvec(self.outp, self.rmap, self.p, self.k, self.comm, self.suffix)
        if self.rank < self.b:
            outputvec(self.outq, self.cmap, self.q, self.k, self.comm, self.suffix)
