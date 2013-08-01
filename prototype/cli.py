import sys 
import socket 
from pykv import pykv

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except socket.error, msg:
    print 'Failed to C a socket. Error code: ' + str(msg[0]) + ' , Msg : ' + msg[1]
    sys.exit()

s.connect(('localhost', 7900))

fid = s.recv(4096)

s.sendall(fid)
s.sendall('set key value')
