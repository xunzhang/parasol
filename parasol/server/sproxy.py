#! /usr/bin/python

#import cPickle
import msgpack as mp
from pykv import pykv
from config import kvpoll_lst 

class sproxy(Exception):
    
    def __init__(self, fd):
        self.fd = fd
	#self.srv_clock = 0
	self.clock_dict = {}
	#self.push('serverclock', 0)
	#self.clt_sz = 0
    
    def clt_sz_push(self, val):
        #self.clt_sz = int(val)
        kvpoll_lst[self.fd].set('clt_sz', int(val))
	return True

    def clt_sz_inc(self, delta_val):
        #self.clt_sz += int(delta_val)
        kvpoll_lst[self.fd].incr('clt_sz', int(delta_val))
	print 'finally', kvpoll_lst[self.fd].get('clt_sz')
	return True
    
    def debug(self):
        print 'self sz is ', self.clt_sz
  
    def debug2(self):
        print 'serverclocks is ', self.pull('serverclock')
             
    def push(self, key, val):
        kvpoll_lst[self.fd].set(key, val)
    
    def push_multi(self, kvdict):
        kvpoll_lst[self.fd].set_multi(kvdict)
    
    def pull(self, key):
        return kvpoll_lst[self.fd].get(key)
         
    def pull_multi(self, keylst):
        return kvpoll_lst[self.fd].get_multi(keylst)
   
    def client_clock_inc(self, key): 
	#if self.clock_dict.get(key):
        if kvpoll_lst[self.fd].get(key):
	    #self.clock_dict[key] += 1
	    kvpoll_lst[self.fd].incr(key, 1)
	else:
	    kvpoll_lst[self.fd].set(key, 1)
	    #self.clock_dict[key] = 1
	#if self.clock_dict['clientclock_' + str(self.srv_clock)] == self.clt_sz:
	#if self.clock_dict[key] >= kvpoll_lst[self.fd].get('clt_sz'):
	if kvpoll_lst[self.fd].get(key) >= kvpoll_lst[self.fd].get('clt_sz'):
	    #if self.clock_dict[key] > self.clt_sz:
            #    print 'debug', 'clt_sz', self.clt_sz
	    #	print 'debug', 'differenb', self.clock_dict[key] - self.clt_sz
	    self.inc('serverclock', 1)
	    #self.clock_dict[key] = 0
	    kvpoll_lst[self.fd].set(key, 0)
        return True
    
    def inc(self, key, delta):
        return kvpoll_lst[self.fd].incr(key, delta)
    
    def pushs(self, key):
        return kvpoll_lst[self.fd].gets(key)
    
    def pulls(self, key, val, uniq):
        return kvpoll_lst[self.fd].cas(key, val, uniq)
    
    def remove(self, key):
        kvpoll_lst[self.fd].delete(key)
    
    def clear(self):
        kvpoll_lst[self.fd].finalize() 

    def pull_all(self):
        return kvpoll_lst[self.fd].getall()
    
    def parser(self, st):
        l = st.split('parasol')
        #oplst = [cPickle.loads(ii) for ii in l]
        oplst = [mp.unpackb(ii) for ii in l]
        op = oplst[0]

        if oplst[-1] != self.fd:
            print 'error'
            exit(0)
        if op == 'push':
            self.push(oplst[1], oplst[2])
        if op == 'push_multi':
            self.push_multi(oplst[1])
        if op == 'pull':
            return self.pull(oplst[1])
        if op == 'pull_multi':
            return self.pull_multi(oplst[1])
        if op == 'inc':
           return self.inc(oplst[1], oplst[2]) 
        if op == 'pushs':
            return self.pushs(oplst[1])
        if op == 'pulls':
            return self.pulls(oplst[1], oplst[2], oplst[3])
        if op == 'remove':
            self.remove(oplst[1])
        if op == 'clear':
            self.clear()
 
if __name__ == '__main__':
    fid = 7
    recvopmsg = "S'set'\np1\n.\tS'hello'\np1\n.\tI5\n.\tI7\n."
    obj = sproxy(fid)
    obj.parser(recvopmsg)
