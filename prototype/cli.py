import sys 
import time
import socket 
from pykv import pykv
from cproxy import cproxy

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
 
    def cserviceinit(self):
        return self.cp
    
    def push(self, key, val):
        self.sock.sendall(self.cp.push(key, val)) 
    
    def pull(self, key):
        self.sock.sendall(self.cp.pull(key))
        print 'send successfully'
        res = self.sock.recv(4096)
        return res
    
if __name__ == '__main__':
    obj = cservice('localhost', 7900)
    # alg code
    obj.push('hello', 'kitty')
    #time.sleep(1)
    print obj.pull('hello')

#try:
#    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#except socket.error, msg:
#    print 'Failed to C a socket. Error code: ' + str(msg[0]) + ' , Msg : ' + msg[1]
#    sys.exit()
#s.connect(('localhost', 7900))
#fid = s.recv(4096)
#s.sendall(fid)
#cp = cproxy(int(fid))
# alg code, cp is init's return 
#st = cp.push('demokey', 10)
# alg code
#st2 = cp.pull('demokey')
#s.sendall(st)
#s.sendall(st2)
