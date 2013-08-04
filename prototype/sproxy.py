#! /usr/bin/python

import cPickle
from pykv import pykv
from config import kvpoll_lst 

class sproxy(Exception):
    
    def __init__(self, fd):
        self.fd = fd
        
    def push(self, key, val):
        kvpoll_lst[self.fd].set(key, val)
    
    def push_multi(self, kvdict):
        kvpoll_lst[self.fd].set_multi(kvdict)
    
    def pull(self, key):
        return kvpoll_lst[self.fd].get(key)
         
    def pull_multi(self, keylst):
        return kvpoll_lst[self.fd].get_multi(keylst)
    
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
    
    def parser(self, st):
        l = st.split('\t')
        oplst = [cPickle.loads(ii) for ii in l]
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
