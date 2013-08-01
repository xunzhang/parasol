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
    
    sid = conn.recv(1)
    op = conn.recv(4096)
    
    # parse op
    sp = sproxy(int(sid))
    v = sp.parser(op)
    
    conn.sendall(str(v))
    
    while True:
        pass
    conn.close()

while 1:
    threadnum += 1
    #print threadnum
    conn, addr = s.accept() # blocking call
    start_new_thread(clientthread, (conn, str(addr), 0))

s.close()
