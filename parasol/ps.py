#! /usr/bin/python

import os
import zmq
import sys
import json
import random
import numpy as np
from mpi4py import MPI
from parasol.clt import kv
from parasol.writer.writer import outputvec
from parasol.server.hash_ring import HashRing

class parasrv(Exception):

    def __init__(self, comm, hosts_dict_lst):
        self.srv_sz = len(hosts_dict_lst)
        #srv_lst = []
        #self.dict_lst = []
        #if comm.Get_rank() == 0:
            #context = zmq.Context()
            #sock = context.socket(zmq.REP)
            #sock.bind("tcp://*:7777")
            #for i in xrange(self.srv_sz):
            #    msg = sock.recv()
            #    tmp = msg.split('parasol')
            #    srv_lst.append((tmp[0], tmp[1]))
            #    sock.send('done')
            #for i in xrange(self.srv_sz):
            #    tmp = {}
            #    tmp["node"] = srv_lst[i][0]
            #    tmp["port"] = srv_lst[i][1]
            #    self.dict_lst.append(tmp)
        self.dict_lst = comm.bcast(hosts_dict_lst, root = 0)
        # generate kvm
        self.ge_kvm(hosts_dict_lst)
        self.servers = [i for i in xrange(self.srv_sz)]
        self.ring = HashRing(self.servers)
        
    def ge_kvm(self, dict_lst):
        self.kvm = [kv(srv['node'], srv['port']) for srv in dict_lst]

class paralg(parasrv):
     
    def __init__(self, comm, hosts_dict_lst):
        parasrv.__init__(self, comm, hosts_dict_lst)
        #self.para_cfg = json.loads(open(para_cfg_file).read())
        #self.srv_sz = self.para_cfg['nserver']
        self.comm = comm
        self.ge_suffix()
        self.comm.barrier() 
    
    def loadinput(self, filename, parser = (lambda l : l), pattern = 'linesplit', mix = False):
        from parasol.loader.crtblkmtx import ge_blkmtx
    	if pattern == 'linesplit':
	    self.linelst = ge_blkmtx(filename, self.comm, parser, pattern, mix)
        else:
	    self.rmap, self.cmap, self.dmap, self.mtx = ge_blkmtx(filename, self.comm, parser, pattern, mix)
     
    def ge_suffix(self):
        suffix = ''
        if self.comm.Get_rank() == 0:
            suffix = '_' + str(random.randint(0, 100000))
        self.suffix = self.comm.bcast(suffix, root = 0)
        
    def crt_outfolder(self, folder): 
        if self.comm.Get_rank() == 0:
            if not os.path.exists(folder):
                os.system('mkdir ' + folder)
    
    def paralg_read(self, key):
        return self.kvm[self.ring.get_node(key)].pull(key)
    
    def __paralg_pack_batch_read(self, valfunc, keyfunc, stripfunc, sz):
        lst_dict = {}
        for i in xrange(self.srv_sz):
            lst_dict[i] = []
        for index in xrange(sz):
            key = keyfunc(str(index))
            server_index = self.ring.get_node(key)
            lst_dict[server_index].append(key)
        keys = []
        tmp = []
        for i in xrange(self.srv_sz):
            if lst_dict[i]:
                keys.append(lst_dict[i])
                tmp.append(self.kvm[i].pull_multi(lst_dict[i]))
        if len(keys) != len(tmp):
            print 'bug in __paralg_pack_batch_write.'
            sys.exit(1)
        for i in xrange(len(keys)):
            for j in xrange(len(keys[i])):
                index = int(stripfunc(keys[i][j])) 
                valfunc(index, tmp[i][j])
        
    def paralg_batch_read(self, valfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), stripfunc = '', sz = 2, pack_flag = True):
        if pack_flag and stripfunc:
            self.__paralg_pack_batch_read(valfunc, keyfunc, stripfunc, sz)
        else:
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                valfunc(index, self.kvm[server_index].pull(key))
                #valfunc(index) = self.kvm[server_index].pull(key)
            
    def paralg_write(self, key, val):
	if isinstance(val, np.ndarry):
	    val = list(val)
        self.kvm[self.ring.get_node(key)].push(key, val)

    def __paralg_pack_batch_write(self, valfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2):
        dict_dict = {}
        for i in xrange(self.srv_sz):
            dict_dict[i] = {}
        # bundle
        for index in xrange(sz):
            key = keyfunc(str(index))
            server_index = self.ring.get_node(key)
            dict_dict[server_index][key] = valfunc(index)
        # real push
        for i in xrange(self.srv_sz):
            if dict_dict[i]:
                self.kvm[i].push_multi(dict_dict[i])
     
    def paralg_batch_write(self, valfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2, pack_flag = True):
        if pack_flag:
            self.__paralg_pack_batch_write(valfunc, keyfunc, sz)
        else:
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                self.kvm[server_index].push(key, valfunc(index))
         
    def paralg_inc(self, key, delta):
	if isinstance(delta, np.ndarry):
	    delta = list(delta)
        self.kvm[self.ring.get_node(key)].update(key, delta)

    # p = np.random.rand(5, 2)
    # print (lambda x : list(p[x,:]))(1)
    def paralg_batch_inc(self, deltafunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2):
        for index in xrange(sz):
            key = keyfunc(str(index))
            server_index = self.ring.get_node(key)
            delta_row = deltafunc(index)
            self.kvm[server_index].update(key, delta_row)
    
    def paralg_batch_inc_nodelta(self, newvalfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2):
        #if newvalfunc(0) != np.ndarray:
	if not isinstance(newvalfunc, np.ndarray):
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                delta_row = list(newvalfunc(index) - self.kvm[server_index].pull(key))
                self.kvm[server_index].update(key, delta_row)
        else:        
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                delta_row = newvalfunc(index) - self.kvm[server_index].pull(key)
                self.kvm[server_index].update(key, delta_row)
    
    def solve(self):
        pass    
    
    def calc_loss(self, calfunc):
        esum = calfunc()
        self.comm.barrier()
        esum = self.comm.allreduce(esum, op = MPI.SUM)
        return esum
    
    def write_result(self):
        pass
    
    def packops(self):
        pass

    def create_row_kv(self, coo_mtx):
       self.paralg()
    
    def create_col_kv(self, coo_mtx):
    	pass 
