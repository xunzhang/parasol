#! /usr/bin/python
#
# to generate ../config/srv_cfg.json
#
def initparasol(Exception):
    import zmq
    import json
    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.bind("tcp://*:7777")
    srv_lst = []
    while i in xrange():
        msg = sock.recv()
        tmp = msg.split('parasol')
        srv_lst.append((tmp[0], tmp[1]))
        sock.send('done')
    print srv_lst
    dict_lst = []
    for i in xrange(len(srv_lst)):
        tmp = {}
        tmp["node"] = srv_lst[i][0]
        tmp["port"] = srv_lst[i][1]
        dict_lst.append(tmp)
        
    # write to srv_cfg.json
    f = open('../config/srv_cfg.json')
    f.write(json.dumps(dict_lst)) 
