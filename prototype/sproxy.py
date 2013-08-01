#! /usr/bin/python

import cPickle
from pykv import pykv
from config import kvpoll_lst 

class sproxy(Exception):
    
    def __init__(self, fd):
        self.fd = fd
        
    def push(self, key, val):
        kvpoll_lst[self.fd].set(key, val)

    def pull(self, key):
        print 'pulll'
        return kvpoll_lst[self.fd].get(key)
         
    def parser(self, st):
        l = st.split('\t')
        print l
        oplst = [cPickle.loads(i) for i in l]
        op = oplst[0]
        if oplst[-1] != self.fd:
            print 'error'
            exit(0)
        if op == 'set':
            self.push(oplst[1], oplst[2])
        if op == 'get':
            return self.pull(oplst[1])
        if op == 'set_multi':
            pass 
         
if __name__ == '__main__':
    fid = 7
    recvopmsg = "S'set'\np1\n.\tS'hello'\np1\n.\tI5\n.\tI7\n."
    obj = sproxy(fid)
    obj.parser(recvopmsg)
