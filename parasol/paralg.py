#! /usr/bin/python

import os
import json
import random
from mpi4py import MPI
from parasol.clt import kv
from parasol.writer.writer import outputvec
from parasol.server.hash_ring import HashRing

class paralg(Exception):
     
    def __init__(self, comm, srv_cfg_file):
        # var
        self.comm = comm
        self.srv_cfg_file = srv_cfg_file
        # op
        self.ge_suffix()
        self.ge_kvm()
        self.srv_sz = len(self.kvm)
        self.servers = [i for i in xrange(self.srv_sz)]
        self.ring = HashRing(self.servers)
        # sync 
        self.comm.barrier() 
     
    def __start_srvs(self):
        pass
 
    def ge_suffix(self):
        suffix = ''
        if self.comm.Get_rank() == 0:
            suffix = '_' + str(random.randint(0, 100000))
        self.suffix = self.comm.bcast(suffix, root = 0)
   
    def ge_kvm(self):
        self.kvm = [kv(srv['node'], srv['port']) for srv in json.loads(open(self.srv_cfg_file).read())]
        #self.__start_srvs()
        
    def crt_outfolder(self, folder): 
        if self.comm.Get_rank() == 0:
            if not os.path.exists(folder):
                os.system('mkdir ' + folder)
    
    def solve(self):
        pass    

    def packops(self):
        pass
