#! /usr/bin/python
import zmq

def get_hostnames_st(srv_sz):
    st = ''
    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.bind('tcp://*:7777')
    for i in xrange(srv_sz):
        msg = sock.recv()
        tmp = msg.split('parasol')
        #srv_lst.append((tmp[0], tmp[1]))
	st += tmp[0] + ':' + tmp[1]
	if i != srv_sz - 1:
		st += 'parasol'
        sock.send('done')
    return st 

def get_hostnames_dict(names):
    lst = names.split('parasol')
    dict_lst = []
    for item in lst:
    	l = item.split(':')
        tmp = {}
        tmp['node'] = l[0]
        tmp['port'] = l[1]
        dict_lst.append(tmp)
    return dict_lst
