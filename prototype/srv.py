import socket
import sys
from thread import *
from pykv import pykv

HOST = ''       # Symbolic name meaning all available interfaces
PORT = 7900     

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    s.bind((HOST, PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
    
s.listen(10)

#Function for handling connections. This will be used to create threads
def clientthread(conn, addr, offset):
    conn.sendall(str(offset))
    #kvstore_lst[0].set('hello', 'world') 
    sid = conn.recv(1)
    op = conn.recv(4096)
    print sid
    print op
    #print kvstore_lst[int(sid)].get('hello')
    # parse op
    operation = op.split(' ')
    print operation
    if operation[0] == 'set':
        kvstore_lst[int(sid)].set(operation[1], operation[2])
    print kvstore_lst[int(sid)].get(operation[1])
    while True:
        pass
    conn.close()

kvstore_lst = []
for i in xrange(10):
    kvstore_lst.append(pykv())
 
theadnum = 0
while 1:
    theadnum += 1
    # blocking call
    conn, addr = s.accept()
    # print 'Connected with ' + addr[0] + ':' + str(addr[1])
    start_new_thread(clientthread, (conn, str(addr), theadnum - 1))
s.close()
