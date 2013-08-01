import socket
import sys
from thread import *
from pykv import pykv
from config import kvpoll_lst, threadnum
from sproxy import sproxy

def cltthrd(conn, addr, index):
    conn.sendall(str(index))
    sid = conn.recv(1)
    op = conn.recv(4096)

    # parse op
    sp = sproxy(int(sid))

    # do the operation
    v = sp.parser(op)

    # only send pull-like result
    if v:
        conn.sendall(str(v))

    conn.close()

if __name__ == '__main__':

    HOST = ''       # Symbolic name meaning all available interfaces
    PORT = 7900     
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, PORT))
    except socket.error , msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    s.listen(6)

    while 1:
        threadnum += 1
        #print threadnum
        conn, addr = s.accept() # blocking call
        start_new_thread(cltthrd, (conn, str(addr), 0))

    s.close()
