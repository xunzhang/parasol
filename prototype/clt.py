import sys 
import socket 
import cPickle
from pykv import pykv
from cproxy import cproxy

class kv(Exception):
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connnum = 0
        self.pushflag = False
        self.pullflag = False 
         
    def push(self, key, val):
        #if not self.pushflag:
        self.pushconn = cservice(self.host, self.port)
        #    self.pushflag = True
        self.pushconn.push(key, val)
    
    def push_multi(self, kvdict):
        conn = cservice(self.host, self.port)
        conn.push_multi(kvdict)
          
    def pull(self, key):
        conn = cservice(self.host, self.port)
        return conn.pull(key)
    
    def pull_multi(self, keylst):
        conn = cservice(self.host, self.port)
        return conn.pull_multi(keylst)
    
    def update(self, key, delta):
        conn = cservice(self.host, self.port)
        return conn.inc(key, delta)
     
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
    
    def __init__(self, host, port):
        try:
             self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error, msg:
            print 'Failed to C a socket. Error code: ' + str(msg[0]) + ' , Msg : ' + msg[1]
            sys.exit()

        self.sock.connect((host, port))
        self.fid = int(self.sock.recv(1))
        self.sock.sendall(str(self.fid))
        self.cp = cproxy(self.fid)
 
    def push(self, key, val):
        self.sock.sendall(self.cp.push(key, val)) 
        
    def push_multi(self, kvdict):
        self.sock.sendall(self.cp.push_multi(kvdict))
    
    def pull(self, key):
        self.sock.sendall(self.cp.pull(key))
        res = cPickle.loads(self.sock.recv(4096))
        return res
    
    def pull_multi(self, keylst):
        self.sock.sendall(self.cp.pull_multi(keylst))
        res = cPickle.loads(self.sock.recv(4096))
        return res
    
    def inc(self, key, delta):
        self.sock.sendall(self.cp.inc(key, delta))
     
    def pushs(self, key):
        self.sock.sendall(self.cp.pushs(key))
        res = cPickle.loads(self.sock.recv(4096))
        return res

    def pulls(self, key, val, uniq):
        self.sock.sendall(self.cp.pulls(key, val, uniq))
        res = cPickle.loads(self.sock.recv(4096))
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
