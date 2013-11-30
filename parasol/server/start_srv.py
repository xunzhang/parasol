#! /usr/bin/python

import sys
import json
import threading
import socket
import zmq
import msgpack as mp
from optparse import OptionParser
from pykv import pykv
from config import kvpoll_lst
from sproxy import sproxy
from parasol.utils.getport import getport, getports

def prepare(init_host):
    import sys
    context = zmq.Context()
    sock_init = context.socket(zmq.REQ)
    sock_init.connect('tcp://' + init_host + ':7777')
    portlst = getports()
    portlst_str = ''
    for pt in portlst[:-1]:
        portlst_str += str(pt) + ','
    portlst_str += str(portlst[-1])
    sock_init.send(socket.gethostname() + 'parasol' + portlst_str)
    ret = sock_init.recv()
    if ret != 'done':
        print 'error in getting hostnames'
	sys.exit(1)
    return portlst
    
def thread_exec(sock):
    sp = sproxy(0)
    global opmutex
    while 1:
        msg = sock.recv()
	l = msg.split('parasol')
	oplst = [mp.unpackb(ii) for ii in l]
	ind = oplst[0]
	v = ''
	opmutex.acquire()
	if ind == 'pull':
	    v = sp.pull(oplst[1])
	    if v == None:
	        v = 'nokey'
	if ind == 'pull_multi':
	    v = sp.pull_multi(oplst[1])
	if ind == 'pullall':
	    v = sp.pull_all()
        if ind == 'push':
	    if oplst[1] == 'clt_sz':
	        sp.clt_sz_push(oplst[2])
	    else:
	        sp.push(oplst[1], oplst[2])
	if ind == 'push_multi':
	    sp.push_multi(oplst[1])
	if ind == 'inc':
	    if oplst[1] == 'clt_sz':
	        sp.clt_sz_inc(oplst[2])
	    elif oplst[1].startswith('clientclock'):
	        sp.client_clock_inc(oplst[1])
	    else:
	        sp.inc(oplst[1], oplst[2])
	if ind == 'remove':
	    sp.remove(oplst[1])
	if ind == 'clear':
	    sp.clear()
	opmutex.release()
        if v or v == 0:
            content = mp.packb(v)
            sock.send(content)

def multithrd_main(init_host):
    ports = prepare(init_host)

    thrd_tasks = []
    context = zmq.Context()
    
    sock = context.socket(zmq.REP)
    sock.bind('tcp://*:' + str(ports[0]))
    thrd_tasks.append(threading.Thread(target = thread_exec, args = (sock, )))

    for i in [1, 2, 3]:
        sock = context.socket(zmq.PULL)
	sock.bind('tcp://*:' + str(ports[i]))
	thrd_tasks.append(threading.Thread(target = thread_exec, args = (sock, )))

    global opmutex
    for task in thrd_tasks:
        opmutex = threading.Lock()
        task.start()
    for task in thrd_tasks:
        task.join()

if __name__ == '__main__':
    
    optpar = OptionParser()
    optpar.add_option('--hostname', action = 'store', type = 'string', default = 'dwalin', dest = 'init_host', help = 'local host for getting parasrv hostnames', metavar = 'localhost')
    options, args = optpar.parse_args()

    multithrd_main(options.init_host)
