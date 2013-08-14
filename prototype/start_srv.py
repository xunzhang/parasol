import cPickle
from pykv import pykv
from sproxy import sproxy
import zmq

if __name__ == '__main__':
  context = zmq.Context()
  sock = context.socket(zmq.REP)
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

