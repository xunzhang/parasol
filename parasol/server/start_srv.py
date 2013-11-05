#! /usr/bin/python

import sys
import json
import socket
#import cPickle
import zmq
import msgpack as mp
from optparse import OptionParser
from pykv import pykv
from sproxy import sproxy
from parasol.utils.getport import getport

def rselect(gset):
    import random
    return random.choice(list(gset))

if __name__ == '__main__':
    context = zmq.Context()
    sock = context.socket(zmq.REP)
    optpar = OptionParser()
    optpar.add_option('--hostname', action = 'store', type = 'string', default = 'dwalin', dest = 'init_host', help = 'local host for getting parasrv hostnames', metavar = 'localhost')
    options, args = optpar.parse_args()
     
    context_init = zmq.Context()
    sock_init = context.socket(zmq.REQ)
    sock_init.connect('tcp://' + options.init_host + ':7777')
    port = getport()
    #sock_init.send('localhost' + 'parasol' + str(port))
    sock_init.send(socket.gethostname() + 'parasol' + str(port))
    ret = sock_init.recv()
    if ret != 'done':
        print 'error in geting hostnames'
        sys.exit(1)
    sock.bind('tcp://*:' + str(port))
    sp = sproxy(0)
    while True:
        v = None
        message = sock.recv() 
        l = message.split('parasol')
        #oplst = [cPickle.loads(ii) for ii in l]
        oplst = [mp.unpackb(ii) for ii in l]
        #print oplst
        op = oplst[0]
        #sp = sproxy(0)
        if op == 'push':
	    if oplst[1] == 'clt_sz':
                v = sp.clt_sz_push(oplst[2])
            else:
                v = sp.push(oplst[1], oplst[2])
        if op == 'inc':
	    if oplst[1] == 'clt_sz':
                v = sp.clt_sz_inc(oplst[2])
            elif oplst[1].startswith('clientclock'):
                v = sp.client_clock_inc(oplst[1], oplst[2])
            else:
                v = sp.inc(oplst[1], oplst[2])
        if op == 'pull':
            v = sp.pull(oplst[1])
	    if not v:
	        v = 'nokey'
        if op == 'pull_multi':
            v = sp.pull_multi(oplst[1])
        if op == 'push_multi':
            v = sp.push_multi(oplst[1])
	if op == 'pullall':
	    v = sp.pull_all()
	if op == 'pull1by1':
	    dct = sp.pull_all()
	    for kv in dct.items():
		sock.send(mp.packb(kv))
	    sock.send('parasoldone')
        if v or v == 0:
            #content = cPickle.dumps(v)     
            content = mp.packb(v)
            sock.send(content)
        else:
            sock.send('ok')

