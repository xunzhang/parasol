import zmq
#import cPickle
import msgpack as mp
from parasol.server.pykv import pykv
from cproxy import cproxy

class kv(Exception):
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connnum = 0
        self.pushflag = False
        self.pullflag = False 
        self.updateflag = False
        self.push_multiflag = False
        self.pull_multiflag = False
        self.pushsflag = False
        self.pullsflag = False
        self.removeflag =False
        self.clearflag = False
         
    def push(self, key, val):
        if not self.pushflag:
            self.pushconn = cservice(self.host, self.port)
            self.pushflag = True
        self.pushconn.push(key, val)
    
    def push_multi(self, kvdict):
        if not self.push_multiflag:
            self.push_multiconn = cservice(self.host, self.port)
            self.push_multiflag = True
        self.push_multiconn.push_multi(kvdict)
          
    def pull(self, key):
        if not self.pullflag:
            self.pullconn = cservice(self.host, self.port)
            self.pullflag = True
        return self.pullconn.pull(key)
    
    def pull_multi(self, keylst):
        if not self.pull_multiflag:
            self.pull_multiconn = cservice(self.host, self.port)
            self.pull_multiflag = True
        return self.pull_multiconn.pull_multi(keylst)
    
    def update(self, key, delta):
        if not self.updateflag:
            self.updateconn = cservice(self.host, self.port)
            self.updateflag = True
        return self.updateconn.inc(key, delta)
     
    def pushs(self, key):
        if not self.pushsflag:
            self.pushsconn = cservice(self.host, self.port)
            self.pushsflag = True
        return self.pushsconn.pushs(key)

    def pulls(self, key, val, uniq):
        if not self.pullsflag:
            self.pullsconn = cservice(self.host, self.port)
            self.pullsflag = True
        return conn.pulls(key, val, uniq)    
        
    def remove(self, key):
        if not self.removeflag:
            self.removeconn = cservice(self.host, self.port)
            self.removeflag = True
        self.removeconn.remove(key)
        
    def clear(self):
        if not self.clearflag:
            self.clearconn = cservice(self.host, self.port)
            self.clearflag = True
        self.clearconn.clear()
        

class cservice(Exception):

    def __init__(self, host, port):
        context = zmq.Context()
        self.sock = context.socket(zmq.REQ)
        #self.sock.connect("tcp://localhost:7907")
        initst = 'tcp://' + host + ':' + port
        self.sock.connect(initst)
        self.cp = cproxy(0)
 
    def push(self, key, val):
        self.sock.send(self.cp.push(key, val)) 
        ret = self.sock.recv()
        
    def push_multi(self, kvdict):
        self.sock.sendall(self.cp.push_multi(kvdict))
        ret = self.sock.recv()
    
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
