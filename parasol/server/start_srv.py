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
    while True:
        message = sock.recv() 
        l = message.split('parasol')
        #oplst = [cPickle.loads(ii) for ii in l]
        oplst = [mp.unpackb(ii) for ii in l]
        #print oplst
        op = oplst[0]
        sp = sproxy(0)
        if op == 'push':
            v = sp.push(oplst[1], oplst[2])
        if op == 'inc':
            v = sp.inc(oplst[1], oplst[2])
        if op == 'pull':
            v = sp.pull(oplst[1])
        if op == 'pull_multi':
            v = sp.pull_multi(oplst[1])
        if op == 'push_multi':
            v = sp.push_multi(oplst[1])
        if v or v == 0:
            #content = cPickle.dumps(v)     
            content = mp.packb(v)
            sock.send(content)
        else:
            sock.send('ok')

