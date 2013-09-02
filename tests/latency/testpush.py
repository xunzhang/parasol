import sys
sys.path.append('../../../xz_utils/')
sys.path.append('../../parasol/')
sys.path.append('../../alg/')
from mpi4py import MPI
from clt import kv
from hash_ring import HashRing
servers = [0]
#servers = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
ring = HashRing(servers)
kvm = [kv('balin', '7908')]
#kvm = [kv('dwalin', '7907'), kv('balin', '7907'), kv('beater1', '7907'), kv('beater2', '7907'), kv('beater3', '7907'), kv('beater4', '7907'), kv('beater5', '7907'), kv('beater6', '7907'), kv('beater9', '7907'), kv('longholes2', '7907'), kv('longholes3', '7907'), kv('longholes5', '7907')]

cnt = 0
for index in xrange(100000):
    print cnt
    cnt += 1
    key = 'p_' + str(index)
    server = ring.get_node(key)
    kvm[server].push(key, '20130902')
print kvm[server].pull(key)
