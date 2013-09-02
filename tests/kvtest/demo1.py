import sys
sys.path.append('../../alg/')
sys.path.append('../../parasol/')
from hash_ring import HashRing
from clt import kv

servers = [0, 1]
ring = HashRing(servers)

kvm = [kv('dwalin', '7907'), kv('balin', '7907')]

key = 'key4session'
server = ring.get_node(key)
kvm[server].push(key, 2013090202)
print kvm[server].pull(key)
while True:
  a = 0
