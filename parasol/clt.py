import sys 
import socket 
import cPickle
import msgpack as mp
from pykv import pykv
from cproxy import cproxy
import zmq

class kv(Exception):
    
    def __init__(self, host, port, context):
        self.host = host
        self.port = port
        self.connnum = 0
        self.zmq_context = context
        self.pushflag = False
        self.pullflag = False 
        self.updateflag = False
         
    def push(self, key, val):
        if not self.pushflag:
            self.pushconn = cservice(self.host, self.port, self.zmq_context)
            self.pushflag = True
        self.pushconn.push(key, val)
    
    def push_multi(self, kvdict):
        conn = cservice(self.host, self.port)
        conn.push_multi(kvdict)
          
    def pull(self, key):
        if not self.pullflag:
            self.pullconn = cservice(self.host, self.port, self.zmq_context)
            self.pullflag = True
        return self.pullconn.pull(key)
    
    def pull_multi(self, keylst):
        conn = cservice(self.host, self.port)
        return conn.pull_multi(keylst)
    
    def update(self, key, delta):
        if not self.updateflag:
            self.updateconn = cservice(self.host, self.port, self.zmq_context)
            self.updateflag = True
        return self.updateconn.inc(key, delta)
     
    def pushs(self, key):
        conn = cservice(self.host, self.port)
        return conn.pushs(key)

    def pulls(self, key, val, uniq):
        conn = cservice(self.host, self.port)
        return conn.pulls(key, val, uniq)    
        
    def remove(self, key):
        conn = cservice(self.host, self.port)
        conn.remove(key)
        
    def clear(self):
        conn = cservice(self.host, self.port)
        conn.clear()
        
class cservice(Exception):
    
    def __init__(self, host, port, context):
        #context = zmq.Context()
        self.context = context
        self.sock = self.context.socket(zmq.REQ)
        initst = 'tcp://' + host + ':' + port
        #self.sock.connect("tcp://localhost:7907")
        self.sock.connect(initst)
        self.cp = cproxy(0)
 
    def push(self, key, val):
        self.sock.send(self.cp.push(key, val)) 
        ret = self.sock.recv()
        
    def push_multi(self, kvdict):
        self.sock.sendall(self.cp.push_multi(kvdict))
    
    def pull(self, key):
        self.sock.send(self.cp.pull(key))
        #res = cPickle.loads(self.sock.recv())
        res = mp.unpackb(self.sock.recv())
        return res
    
    def pull_multi(self, keylst):
        self.sock.sendall(self.cp.pull_multi(keylst))
        #res = cPickle.loads(self.sock.recv(4096))
        res = mp.unpackb(self.sock.recv())
        return res
    
    def inc(self, key, delta):
        self.sock.send(self.cp.inc(key, delta))
        ret = self.sock.recv()

    def pushs(self, key):
        self.sock.sendall(self.cp.pushs(key))
        #res = cPickle.loads(self.sock.recv(4096))
        res = mp.unpackb(self.sock.recv())
        return res

    def pulls(self, key, val, uniq):
        self.sock.sendall(self.cp.pulls(key, val, uniq))
        #res = cPickle.loads(self.sock.recv(4096))
        res = mp.unpackb(self.sock.recv())
        return res
    
    def remove(self, key):
        self.sock.sendall(self.cp.remove(key))
        
    def clear(self):
        self.sock.sendall(self.cp.clear())
 
if __name__ == '__main__':
    # alg code demo
    storeobj = kv('localhost', 7900)
    storeobj.push('hello', 'kitty')
    print storeobj.pull('hello')
    storeobj.push('key', 'val')
    storeobj.push('matrix', 100)
    print storeobj.pull('key')
    print storeobj.pull('matrix')
    storeobj.push_multi({'JWM': 'WH', 'jiang': 'wu'})
    print storeobj.pull_multi(['JWM', 'jiang'])
    storeobj.remove('jiang')
    print storeobj.pull('jiang')
    storeobj.push('p[1:,]', [0.3,0.4,0.5,0.6])
    print storeobj.pull('p[1:,]')
    print type(storeobj.pull('p[1:,]'))
    print [float(i) for i in storeobj.pull('p[1:,]').strip(']').strip('[').split(',')]
    print type([float(i) for i in storeobj.pull('p[1:,]').strip(']').strip('[').split(',')])
    storeobj.clear()
    print storeobj.clear()
