#! /usr/bin/python

import os
import zmq
import json
import random
from mpi4py import MPI
from parasol.clt import kv
from parasol.writer.writer import outputvec
from parasol.server.hash_ring import HashRing

class parasrv(Exception):

    def __init__(self, comm, srv_sz = 1):
        self.srv_sz = srv_sz
        srv_lst = []
        self.dict_lst = []
        if comm.Get_rank() == 0:
            context = zmq.Context()
            sock = context.socket(zmq.REP)
            sock.bind("tcp://*:7777")
            for i in xrange(self.srv_sz):
                msg = sock.recv()
                tmp = msg.split('parasol')
                srv_lst.append((tmp[0], tmp[1]))
                sock.send('done')
            for i in xrange(self.srv_sz):
                tmp = {}
                tmp["node"] = srv_lst[i][0]
                tmp["port"] = srv_lst[i][1]
                self.dict_lst.append(tmp)
        self.dict_lst = comm.bcast(self.dict_lst, root = 0)
        # generate kvm
        self.ge_kvm()
        self.servers = [i for i in xrange(self.srv_sz)]
        self.ring = HashRing(self.servers)
        
    def ge_kvm(self):
        self.kvm = [kv(srv['node'], srv['port']) for srv in self.dict_lst]

class paralg(parasrv):
     
    def __init__(self, comm, para_cfg_file):
        self.para_cfg = json.loads(open(para_cfg_file).read())
        self.srv_sz = self.para_cfg['nserver']
        parasrv.__init__(self, comm, self.srv_sz)
        self.comm = comm
        self.ge_suffix()
        self.comm.barrier() 
     
    def __start_srvs(self):
        pass
 
    def ge_suffix(self):
        suffix = ''
        if self.comm.Get_rank() == 0:
            suffix = '_' + str(random.randint(0, 100000))
        self.suffix = self.comm.bcast(suffix, root = 0)
   
    #def ge_kvm(self):
    #    self.kvm = [kv(srv['node'], srv['port']) for srv in json.loads(open(self.srv_cfg_file).read())]
        #self.__start_srvs()
        
    def crt_outfolder(self, folder): 
        if self.comm.Get_rank() == 0:
            if not os.path.exists(folder):
                os.system('mkdir ' + folder)
    
    def solve(self):
        pass    

    def packops(self):
        pass
