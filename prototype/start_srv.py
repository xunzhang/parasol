import socket
import sys
import numpy as np
import threading
import thread
import time
import cPickle
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
    # only send pull-like result
    if v or v == 0:
        content = cPickle.dumps(v)
        conn.sendall(content)
    return 
    #conn.close()

class push_handler(threading.Thread):
    def run(self):
        global push_task
        sp = sproxy(init_id)
        while(1):
            if mutex.acquire(1):
                if push_task:
                    entity = push_task.pop()
                    v = sp.push(entity[0], entity[1])
                mutex.release()
        return 

class inc_handler(threading.Thread):
    def run(self):
        global inc_task
        sp = sproxy(init_id)
        while(1):
            if mutex.acquire(1):
                #print 'inc_handler'
                if inc_task:
                    entity = inc_task.pop() 
                    v = sp.inc(entity[0], entity[1])
                mutex.release()
                #time.sleep(5)
        return     

def pull_handler(conn, index, key):
    sp = sproxy(init_id)
    if mutex.acquire(1):
        v = sp.pull(key)
    mutex.release()
    if v or v == 0:
        content = cPickle.dumps(v)
        conn.sendall(content)
    return 

mutex = threading.Lock()
    
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

    push_task = []
    inc_task = []
    init_id = 0
    
    incthd = inc_handler()
    incthd.start()
    pushthd = push_handler()
    pushthd.start()
     
    while 1:
        threadnum += 1
        #print threadnum
        conn, addr = s.accept() # blocking call
        # One thread per client
        
        # Single thread, single client
        #oplst = handle(conn, 0)
        #conn.sendall(str(0))
        #sid = int(conn.recv(1))
        #cmd = conn.recv(4096)
        #l = cmd.split('\t')
        #oplst = [cPickle.loads(ii) for ii in l]
        #if oplst[0] == 'push':
        #    push_task.append((oplst[1], oplst[2]))
        #if oplst[0] == 'inc':
        #    inc_task.append((oplst[1], oplst[2]))
        #if oplst[0] == 'pull':
        #    thread.start_new_thread(pull_handler, (conn, 0, oplst[1]))
        #conn.close()
        cltthrd(conn, 0)
        #print threadnum
    s.close()
