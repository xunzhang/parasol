import sys 
import socket 
from pykv import pykv
from cproxy import cproxy

class kv(Exception):
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
    def push(self, key, val):
        conn = cservice(self.host, self.port)
        conn.push(key, val)
        
    def pull(self, key):
        conn = cservice(self.host, self.port)
        return conn.pull(key)

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
    
    def pull(self, key):
        self.sock.sendall(self.cp.pull(key))
        res = self.sock.recv(4096)
        return res
    
if __name__ == '__main__':
    # alg code demo
    storeobj = kv('localhost', 7900)
    storeobj.push('hello', 'kitty')
    print storeobj.pull('hello')
    storeobj.push('key', 'val')
    storeobj.push('matrix', 100)
    print storeobj.pull('key')
    print storeobj.pull('matrix')
