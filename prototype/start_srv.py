import socket
import sys
import numpy as np
import thread
import time
from pykv import pykv
from config import kvpoll_lst, threadnum
from sproxy import sproxy

def cltthrd(conn, index):
    conn.sendall(str(index))
    sid = conn.recv(1)
    op = conn.recv(4096)

    # parse op
    sp = sproxy(int(sid))
    
    # do the operation
    v = sp.parser(op)
    
    #print v
    #print type(v)
    #print type(np.array([1]))
    # only send pull-like result
    #if type(v) == np.array([1]):
    #print 'yep'
    conn.sendall(str(v).replace('     ', ' ').replace('    ', ' ').replace('   ', ' ').replace('  ', ' ').replace('[ ', '[').replace(' ]', ']'))
    #elif v or v == 0:
    #    print 'yes'
    #    conn.sendall(str(v).replace(' ', ''))

    #conn.close()

if __name__ == '__main__':

    HOST = ''       # Symbolic name meaning all available interfaces
    PORT = 7900     
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, PORT))
    except socket.error , msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    s.listen(1)

    while 1:
        threadnum += 1
        #print threadnum
        conn, addr = s.accept() # blocking call
        thread.start_new_thread(cltthrd, (conn, 0))

    s.close()
