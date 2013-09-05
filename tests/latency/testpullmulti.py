import sys
sys.path.append('../../parasol/utils/')
sys.path.append('../../parasol/')
sys.path.append('../../parasol/alg/')
sys.path.append('../../parasol/server/')
from mpi4py import MPI
from clt import kv
from hash_ring import HashRing
servers = [0]
#servers = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
ring = HashRing(servers)
kvm = [kv('dwalin', '8907')]
#kvm = [kv('dwalin', '7907'), kv('balin', '7907'), kv('beater1', '7907'), kv('beater2', '7907'), kv('beater3', '7907'), kv('beater4', '7907'), kv('beater5', '7907'), kv('beater6', '7907'), kv('beater9', '7907'), kv('longholes2', '7907'), kv('longholes3', '7907'), kv('longholes5', '7907')]

srv_sz = 1
plst_dict = {}
for i in xrange(srv_sz):
    plst_dict[i] = []
for index in xrange(100000):
    key = 'p_' + str(index)
    server = ring.get_node(key)
    plst_dict[server].append(key)
pkeys = []
ptmp = []
for i in xrange(srv_sz):
    if plst_dict[i]:
        ptmp.append(kvm[i].pull_multi(plst_dict[i]))
        for ii in plst_dict[i]:
            pkeys.append(ii)
for i in xrange(len(pkeys)):
    index = int(pkeys[i].strip('p_'))
    a = ptmp[0][i]
