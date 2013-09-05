# blocked matrix factorization using stochastic gradient 

import sys
import json
import numpy as np
from mpi4py import MPI
import random
from time import clock
from parasol.utils.parallel import npfact2D 
from parasol.writer.writer import outputvec    
from parasol.loader.crtblkmtx import ge_blkmtx
from parasol.paralg import paralg
    
class bsmf(paralg):

    def __init__(self, comm, srv_cfg_file, para_cfg_file):
        paralg.__init__(self, comm, srv_cfg_file)
        self.rank = self.comm.Get_rank()
        self.para_cfg = json.loads(open(para_cfg_file).read())
        
        self.a, self.b = npfact2D(self.para_cfg['n'])
        self.k = self.para_cfg['k']
        self.filename = self.para_cfg['input']
        self.outp = self.para_cfg['outputp']
        self.outq = self.para_cfg['outputq']
        
        # set default parameter for learning
        self.alpha = 0.0002
        self.beta = 0.02
        self.rounds = 3
         
        alpha = self.para_cfg.get('alpha')
        if alpha:
            self.alpha = alpha
        beta = self.para_cfg.get('beta')
        if beta:
            self.beta = beta
        rounds = self.para_cfg.get('rounds')
        if rounds:
            self.rounds = rounds
        
        # create folder
        paralg.crt_outfolder(self, self.outp)
        paralg.crt_outfolder(self, self.outq)
        self.comm.barrier()
    
    def __stripoff(self, string, suffix, l, r):
        return string.strip(suffix).strip(l).strip(r)
 
    def __pack_op_1(self, sz1, sz2, ind):
        # for p 
        pdict_dict = {} # for push
        plst_dict = {} # for pull
        for i in xrange(self.srv_sz):
            pdict_dict[i] = {}
            plst_dict[i] = []
        if ind == 'push': 
            # bundle
            for index in xrange(sz1):
                key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
                server = self.ring.get_node(key)
                pdict_dict[server][key] = list(self.p[index, :])
            # real push
            for i in xrange(self.srv_sz):
                if pdict_dict[i]:
                    self.kvm[i].push_multi(pdict_dict[i])
        if ind == 'pull':
            for index in xrange(sz1):
                key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
                server = self.ring.get_node(key)
                plst_dict[server].append(key)
            pkeys = []
            ptmp = []
            for i in xrange(self.srv_sz):
                if plst_dict[i]:
                    pkeys.append(plst_dict[i])
                    ptmp.append(self.kvm[i].pull_multi(plst_dict[i]))
                    #for ii in plst_dict[i]:
                    #    pkeys.append(ii)
            if len(pkeys) != len(ptmp):
                print 'bug in __pack_op_1.'
                sys.exit(1)
            for i in xrange(len(pkeys)):
                for j in xrange(len(pkeys[i])):
                    index = int(self.__stripoff(pkeys[i][j], str(self.rank / self.b), 'p[', ',:]_'))
                    self.p[index, :] = ptmp[i][j]
        # for q 
        qdict_dict = {}
        qlst_dict = {}
        for i in xrange(self.srv_sz):
            qdict_dict[i] = {}
            qlst_dict[i] = []
        if ind == 'push': 
            # bundle
            for index in xrange(sz2):
                key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
                server = self.ring.get_node(key)
                qdict_dict[server][key] = list(self.q[:, index])
            # real push
            for i in xrange(self.srv_sz):
                if qdict_dict[i]:
                    self.kvm[i].push_multi(qdict_dict[i])
        if ind == 'pull':
            for index in xrange(sz2):
                key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
                server = self.ring.get_node(key)
                qlst_dict[server].append(key)
            qkeys = []
            qtmp = []
            for i in xrange(self.srv_sz):
                if qlst_dict[i]:
                    qkeys.append(qlst_dict[i])
                    qtmp.append(self.kvm[i].pull_multi(qlst_dict[i]))
                    #for ii in qlst_dict[i]:
                    #    qkeys.append(ii)
            if len(qkeys) != len(qtmp):
                print 'bug in __pack_op_1.'
                sys.exit(1) 
            for i in xrange(len(qkeys)):
                for j in xrange(len(qkeys[i])):
                    index = int(self.__stripoff(qkeys[i][j], str(self.rank % self.b), 'q[:,', ']_'))
                    self.q[:, index] = qtmp[i][j]
    
    def __group_op_1(self, sz1, sz2, ind):
        if ind == 'push':
            for index in xrange(sz1):
                key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
                server = self.ring.get_node(key)
                self.kvm[server].push(key, list(self.p[index, :]))
            for index in xrange(sz2):
                key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
                server = self.ring.get_node(key)
                self.kvm[server].push(key, list(self.q[:, index]))
        if ind == 'pull':
            for index in xrange(sz1):
                key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
                server = self.ring.get_node(key)
                self.p[index, :] = self.kvm[server].pull(key)
            for index in xrange(sz2):
                key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
                server = self.ring.get_node(key)
                self.q[:, index] = self.kvm[server].pull(key)
                        
    def __group_op_2(self, sz1, sz2):
        for index in xrange(sz1):
            key = 'p[' + str(index) + ',:]_' + str(self.rank / self.b)
            server = self.ring.get_node(key)
            deltap = list(self.p[index, :] - self.kvm[server].pull(key))
            self.kvm[server].update(key, deltap)
        for index in xrange(sz2):
            key = 'q[:,' + str(index) + ']_' + str(self.rank % self.b)
            server = self.ring.get_node(key)
            deltaq = list(self.q[:, index] - self.kvm[server].pull(key))
            self.kvm[server].update(key, deltaq)
    
    def __mf_kernel(self):#, alpha = 0.0002, beta = 0.02, rounds = 5):
        pl_sz = self.p.shape[0]
        ql_sz = self.q.shape[1]
        data_container = zip(self.mtx.row, self.mtx.col, self.mtx.data)
        print 'data size is', len(data_container)
        for it in xrange(self.rounds):
            print 'round', it
            if it != 0:
                self.__group_op_1(pl_sz, ql_sz, 'pull')
                #self.__pack_op_1(pl_sz, ql_sz, 'pull')
            print 'after round pull'
            
            start = clock()
            random.shuffle(data_container)
            end = clock()
            print 'shuffle time is', end - start
            
            for i, j, v in data_container:
                eij = v - np.dot(self.p[i, :], self.q[:, j])
                for ki in xrange(self.k):
                    self.p[i][ki] += self.alpha * (2 * eij * self.q[ki][j] - self.beta * self.p[i][ki])
                    self.q[ki][j] += self.alpha * (2 * eij * self.p[i][ki] - self.beta * self.q[ki][j])
            start = clock()
            print 'local calc time is', start - end
            
            self.__group_op_2(pl_sz, ql_sz)
            end = clock()
            print 'communication time is', end - start
    
        # last pull p, may not calc on this procs, but can be calced on others
        self.comm.barrier()
        start = clock()
        self.__group_op_1(pl_sz, ql_sz, 'pull')
        #self.__pack_op_1(pl_sz, ql_sz, 'pull')
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
        self.__group_op_1(u, i, 'push')
        #self.__pack_op_1(u, i, 'push')
        print 'finish init push'
        self.comm.barrier()

        # kernel mf solver
        self.__mf_kernel()

    def solve(self):
        self.rmap, self.cmap, self.mtx = ge_blkmtx(self.filename, self.comm)
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
