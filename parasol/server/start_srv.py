import socket
import cPickle
from pykv import pykv
from sproxy import sproxy
import zmq

def rselect(gset):
  import random
  return random.choice(list(gset))

if __name__ == '__main__':
  context = zmq.Context()
  sock = context.socket(zmq.REP)
  global_index = set(range(100))
  if socket.gethostname() == 'dwalin':
    sock.bind("tcp://*:8907")
    msg = sock.recv()
    kvpoll_index = rselect(global_index)
    sock.send(str(kvpoll_index))
    global_index.remove(kvpoll_index)
  # tell all srv global_index
  
  sock.bind("tcp://*:7907")
  while True:
    message = sock.recv() 
    l = message.split('\t')
    oplst = [cPickle.loads(ii) for ii in l]
    op = oplst[0]
    sp = sproxy(0)
    if op == 'push':
        v = sp.push(oplst[1], oplst[2])
    if op == 'inc':
        v = sp.inc(oplst[1], oplst[2])
    if op == 'pull':
        v = sp.pull(oplst[1])
    if v or v == 0:
        content = cPickle.dumps(v)     
        sock.send(content)
    else:
        sock.send('ok')

