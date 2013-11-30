import zmq
import msgpack as mp
from parasol.server.pykv import pykv
from cproxy import cproxy

class kv(Exception):
    
    def __init__(self, host, ports):
	self.context = zmq.Context()
	self.cp = cproxy(0)
	self.conn_info = 'tcp://' + host + ':'# + ports
	self.ports = ports.split(',')
        self.pullflag = False 
        self.pull_multiflag = False
	self.pullallflag = False
        self.pushflag = False
        self.push_multiflag = False
        self.updateflag = False
        self.pushsflag = False
        self.pullsflag = False
        self.removeflag =False
        self.clearflag = False
         
    def _create_req_sock(self, port):
        sock = self.context.socket(zmq.REQ)
	sock.connect(self.conn_info + port)
	return sock

    def _create_push_sock(self, port):
        sock = self.context.socket(zmq.PUSH)
	sock.connect(self.conn_info + port)
	return sock

    def pull(self, key):
        if not self.pullflag:
            self.pullflag = True
	    self.pullsock = self._create_req_sock(self.ports[0])
	self.pullsock.send(self.cp.pull(key))
        return mp.unpackb(self.pullsock.recv())

    def pull_multi(self, keylst):
        if not self.pull_multiflag:
            self.pull_multiflag = True
	    self.pull_multisock = self._create_req_sock(self.ports[0])
	self.pull_multisock.send(self.cp.pull_multi(keylst))
        return mp.unpackb(self.pull_multisock.recv())
    
    def pullall(self):
        if not self.pullallflag:
	    self.pullallflag = True
	    self.pullallsock = self._create_req_sock(self.ports[0])
	self.pullallsock.send(self.cp.pullall())
	return mp.unpackb(self.pullallsock.recv())

    def push(self, key, val):
        if not self.pushflag:
            self.pushflag = True
	    self.pushsock = self._create_req_sock(self.ports[1])
	self.pushsock.send(self.cp.push(key, val))
	self.pushsock.recv()
    
    def push_multi(self, kvdict):
        if not self.push_multiflag:
            self.push_multiflag = True
	    self.push_multisock = self._create_push_sock(self.ports[1])
	self.push_multisock.send(self.cp.push_multi(kvdict))
          
    def update(self, key, delta):
        if not self.updateflag:
            self.updateflag = True
	    self.updatesock = self._create_push_sock(self.ports[2])
	self.updatesock.send(self.cp.inc(key, delta))
    
    def pushs(self, key):
        if not self.pushsflag:
            self.pushsflag = True
	    self.pushssock = self._create_push_sock('7777')
	self.pushssock.send(self.cp.pushs(key))
        self.pushssock.recv()

    def pulls(self, key, val, uniq):
        if not self.pullsflag:
            self.pullsflag = True
	    self.pullssock = self._create_req_sock('7777')
	self.pullssock.send(self.cp.pulls(key, val, uniq))
        return mp.unpackb(self.pullssock.recv())
        
    def remove(self, key):
        if not self.removeflag:
            self.removeflag = True
	    self.removesock = self._create_push_sock(self.ports[3])
        self.removesock.send(self.cp.remove(key))
	self.removesock.recv()
        
    def clear(self):
        if not self.clearflag:
            self.clearflag = True
	    self.clearsock = self._create_push_sock(self.ports[3])
        self.clearsock.send(self.cp.clear())
	self.clearsock.recv()
