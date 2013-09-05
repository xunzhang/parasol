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

cnt = 0
srv_sz = 1
pdict_dict = {}
for i in xrange(srv_sz):
    pdict_dict[i] = {}
#print kvm[server].pull(key)
for index in xrange(100000):
    key = 'p_' + str(index)
    server = ring.get_node(key)
    pdict_dict[server][key] = list([20130905, 2010210827])
for i in xrange(srv_sz):
    if pdict_dict[i]:
        kvm[i].push_multi(pdict_dict[i])
    
