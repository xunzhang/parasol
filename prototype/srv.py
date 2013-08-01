import socket
import sys
from thread import *
from pykv import pykv
from config import kvpoll_lst, threadnum
from sproxy import sproxy

HOST = ''       # Symbolic name meaning all available interfaces
PORT = 7900     

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    s.bind((HOST, PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
    
s.listen(6)

#Function for handling connections. This will be used to create threads
def clientthread(conn, addr, index):
    conn.sendall(str(index))
    #kvpoll_lst[0].set('hello', 'world')
    sid = conn.recv(1)
    print 's1'
    op = conn.recv(4096)
    print op
    print 's2'
    op2 = conn.recv(4096)
    print op2
    print 's3'
    # print kvpoll_lst[int(sid)].get('hello')
    # parse op
    sp = sproxy(int(sid))
    sp.parser(op)
    print 'be'
    v = sp.parser(op2)
    print v
    conn.sendall(str(v))
    #operation = op.split(' ')
    #if operation[0] == 'set':
    #    kvpoll_lst[int(sid)].set(operation[1], operation[2])
    #print kvpoll_lst[int(sid)].get(operation[1])
    while True:
        pass
    conn.close()

while 1:
    threadnum += 1
    print threadnum
    conn, addr = s.accept() # blocking call
    start_new_thread(clientthread, (conn, str(addr), threadnum - 1))

s.close()
