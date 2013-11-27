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
        self.dict_lst = comm.bcast(hosts_dict_lst, root = 0)
        # generate kvm
        self.ge_kvm()
        self.servers = [i for i in xrange(self.srv_sz)]
        self.ring = HashRing(self.servers)
        
    def ge_kvm(self):
        self.kvm = [kv(srv['node'], srv['ports']) for srv in self.dict_lst]

class paralg(parasrv):
     
    def __init__(self, comm, hosts_dict_lst, nworker, rounds = 1, limit_s = 1):
        self.nworker = nworker
        parasrv.__init__(self, comm, hosts_dict_lst)
	self.clock = 0
	self.stale_cache = 0
	self.rounds = rounds
	self.limit_s = limit_s
        self.comm = comm
	self.cached_para = {}
        self.clockserver = 0
	self.dataset_sz = 0
	self.linelst = []
	self.all_kvs = {}
	if self.comm.Get_rank() == 0:
            self.kvm[self.clockserver].push('clt_sz', self.nworker)
        self.ge_suffix()
        self.comm.barrier() 
	 
    def loadinput(self, filename, parser = (lambda l : l), pattern = 'linesplit', mix = False):
    	from parasol.loader import loader
	self.pattern = pattern
	ld = loader(filename, self.comm, pattern, parser, mix)
	self.linelst = ld.load()
	self.dataset_sz = len(self.linelst) * self.rounds
	if pattern != 'linesplit':
	    self.graph, self.rmap, self.cmap, self.dmap, self.col_dmap = ld.create_graph(self.linelst)
	    self.dimx = len(self.rmap)
	    self.dimy = len(self.cmap)
	    self.dataset_sz = self.rounds 
	    #self.dataset_sz = self.mtx.shape[0] * self.rounds
       
    def get_dataset_dim(self):
        '''
	return graph size, default 2d
	'''
        return self.dimx, self.dimy
    
    def get_graph(self):
        '''
        return zip data
        '''
        return self.graph

    def getlines(self):
        '''
	return lines
	'''
        return self.linelst

    def set_steps(self, sz):
        '''
	set phase size to work with ssp
	'''
        self.dataset_sz = sz

    def sync(self):
        self.comm.barrier()

    def get_nodeid(self):
        '''
	return procs name(not pid)
	'''
        return self.comm.Get_rank()

    def get_nodesize(self):
        '''
	return number of workers
	'''
        return self.comm.Get_size()

    def get_decomp(self):
        '''
	must be called after loadinput
	return decomp dims
	'''
        from decomp import npfactx, npfacty, npfact2d
	if self.pattern == 'linesplit':
	    return npx
        npx, npy = npfactx(self.nworker)
        if self.pattern == 'fsmap':
	    npx, npy = npfact2d(self.nworker)
	if self.pattern == 'smap':
	    npx, npy = npfacty(self.nworker)
        return npx, npy

    def ge_suffix(self):
        suffix = ''
        if self.comm.Get_rank() == 0:
            suffix = '_' + str(random.randint(0, 100000))
        self.suffix = self.comm.bcast(suffix, root = 0)
        
    def crt_outfolder(self, folder): 
        if self.comm.Get_rank() == 0:
            if not os.path.exists(folder):
                os.system('mkdir ' + folder)
   
    def iter_done(self):
        if self.limit_s == 0:
	    clock_key = 'clientclock_0'
	else:
	    clock_key = 'clientclock_' + str(self.clock % self.limit_s)
	self.kvm[self.clockserver].update(clock_key, 1)
        self.clock += 1
	if self.clock == self.dataset_sz:
            self.kvm[self.clockserver].update('clt_sz', -1)
    
    def paralg_contains(self, key):
        val = self.kvm[self.ring.get_node(key)].pull(key)
        if val == 'nokey':
	    return False
	self.cached_para[key] = val
	return True

    def paralg_read(self, key):
        if self.clock == 0 or self.clock == self.dataset_sz:
	    self.cached_para[key] = self.kvm[self.ring.get_node(key)].pull(key)
            return self.cached_para[key]
	if self.stale_cache + self.limit_s > self.clock:
	    # cache hit
	    return self.cached_para[key]
        else:
	    # cache miss
            # pull from server until leading slowest less than s clocks
            while self.stale_cache + self.limit_s < self.clock:
                self.stale_cache = self.kvm[self.clockserver].pull('serverclock')
    	    self.cached_para[key] = self.kvm[self.ring.get_node(key)].pull(key)
	    return self.cached_para[key]

    def paralg_batch_read(self, valfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), stripfunc = '', sz = 2, pack_flag = False):
	if self.clock == 0 or self.clock == self.dataset_sz:
	    for index in xrange(sz):
                key = keyfunc(str(index))
	        self.cached_para[key] = self.kvm[self.ring.get_node(key)].pull(key)
                valfunc(index, self.cached_para[key])
	    return
        cache_flag = True
        if self.stale_cache + self.limit_s < self.clock:
            cache_flag = False
	while self.stale_cache + self.limit_s < self.clock:
            self.stale_cache = self.kvm[self.clockserver].pull('serverclock')

        if pack_flag and stripfunc:
            self.__paralg_pack_batch_read(valfunc, keyfunc, stripfunc, sz, cache_flag)
        else:
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
		if cache_flag:
		    valfunc(index, self.cached_para[key])
		else:
                    valfunc(index, self.kvm[server_index].pull(key))
            
    def __paralg_pack_batch_read(self, valfunc, keyfunc, stripfunc, sz, cache_flag):
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
		if cache_flag:
		    tmpvalst = []
                    for tmpkey in lst_dict[i]:
                        tmpvalst.append(self.cached_para[tmpkey])
		    tmp.append(tmpvalst)
		else:
                    tmp.append(self.kvm[i].pull_multi(lst_dict[i]))
        if len(keys) != len(tmp):
            print 'bug in __paralg_pack_batch_write.'
            sys.exit(1)
        for i in xrange(len(keys)):
            for j in xrange(len(keys[i])):
                index = int(stripfunc(keys[i][j])) 
                valfunc(index, tmp[i][j])
        
    def paralg_write(self, key, val):
	if isinstance(val, np.ndarray):
	    val = list(val)
	# assign local para
	self.cached_para[key] = val
        self.kvm[self.ring.get_node(key)].push(key, val)

    def paralg_batch_write(self, valfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2, pack_flag = False):
        if pack_flag:
            self.__paralg_pack_batch_write(valfunc, keyfunc, sz)
        else:
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
		# assign local para
		tmpval = valfunc(index)
		self.cached_para[key] = tmpval
                self.kvm[server_index].push(key, tmpval)
         
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
		# assign local para
		for tmpkey in dict_dict[i].keys():
		    self.cached_para[tmpkey] = dict_dict[i][tmpkey]
                self.kvm[i].push_multi(dict_dict[i])
     
    def paralg_inc(self, key, delta):
	# update delta to local cache, make sure to read-my-writes
	if isinstance(delta, np.ndarray) or isinstance(delta, list):
	    delta = list(delta)
            self.cached_para[key] = [self.cached_para[key][t] + delta[t] for t in xrange(len(delta))]	
	else:
	    self.cached_para[key] += delta
	# send update op to parameter server
        self.kvm[self.ring.get_node(key)].update(key, delta)

    # p = np.random.rand(5, 2)
    # print (lambda x : list(p[x,:]))(1)
    def paralg_batch_inc(self, deltafunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2):
        if isinstance(deltafunc(0), np.ndarray) or isinstance(deltafunc(0), list):
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                delta_row = list(deltafunc(index))
                self.cached_para[key] = [self.cached_para[key][t] + delta_row[t] for t in xrange(len(delta_row))] 
                self.kvm[server_index].update(key, delta_row)
        else:
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                delta_row = deltafunc(index)
	        self.cached_para[key] += delta_row
                self.kvm[server_index].update(key, delta_row)
    
    def paralg_batch_inc_nodelta(self, newvalfunc, keyfunc = (lambda prefix, suffix : lambda index_st : prefix + index_st + suffix)('', ''), sz = 2):
        #if newvalfunc(0) != np.ndarray:
	if isinstance(newvalfunc(0), np.ndarray) or isinstance(newvalfunc(0), list):
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                delta_row = list(newvalfunc(index) - self.kvm[server_index].pull(key))
	        self.cached_para[key] = [self.cached_para[key][t] + delta_row[t] for t in xrange(len(delta_row))]
                self.kvm[server_index].update(key, delta_row)
        else:        
            for index in xrange(sz):
                key = keyfunc(str(index))
                server_index = self.ring.get_node(key)
                delta_row = newvalfunc(index) - self.kvm[server_index].pull(key)
	        self.cached_para[key] += delta_row
                self.kvm[server_index].update(key, delta_row)
   
    def paralg_read_all(self, filter_dict_func = lambda x : x):
        table = {}
        for srvid in xrange(self.srv_sz):
	    table.update(filter_dict_func(self.kvm[srvid].pullall()))
	if table.get('serverclock'):
	    del table['serverclock']
	return table
   
    def paralg_read_topk(self, k, filter_dict_func = lambda x : x):
        table = self.paralg_read_all(filter_dict_func)
	if table.get('serverclock'):
	    del table['serverclock']
	return sorted(table.items(), key = lambda kv : kv[-1], reverse = True)[0:k]

    def paralg_read_btmk(self, k, filter_dict_func = lambda x : x):
        table = self.paralg_read_all(filter_dict_func)
	if table.get('serverclock'):
	    del table['serverclock']
	return sorted(table.items(), key = lambda kv : kv[-1])[0:k]

    def read_topk_limitmem(self, k, filter_pair_func = lambda x : x if x or x == 0 else None):
        from parasol.utils.heap import minhp
        topktable = {}
	topkheap = minhp(k)
	for srvid in xrange(self.srv_sz):
	    kvpair = filter_pair_func(self.kvm[srvid].pull1by1())
	    if kvpair:
	        topkheap.heappush(kvpair)
	for cnt in xrange(k):
            kvpair = topkheap.heappop()
	    topktable[kvpair[0]] = kvpair[1]
        return topktable

    def read_btmk_limitmem(self, k, filter_pair_func = lambda x : x if x or x == 0 else None):
        from parasol.utils.heap import maxhp
	btmktable = {}
	btmkheap = maxhp(k)
	for srvid in xrange(self.srv_sz):
	    kvpair = filter_pair_func(self.kvm[srvid].pull1by1())
	    if kvpair:
	        btmheap.heappush(kvpair)
	for cnt in xrange(k):
	    kvpair = btmkheap.heappop()
	    btmktable[kvpair[0]] = kvpair[1]
	return btmtable

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
