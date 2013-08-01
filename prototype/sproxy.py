#! /usr/bin/python

import cPickle
from pykv import pykv

class sproxy(Exception):
    
    def __init__(self, fd):
        self.fd = fd
    
    def push(self, key, val):
         print 'i am here at push func.'
    
    def parser(self, st):
        l = st.split('\t')
        oplst = [cPickle.loads(i) for i in l]
        op = oplst[0]
        if op == 'set':
            self.push(oplst[1], oplst[2])
        if op == 'get':
            self.pull(oplst[1])
        if op == 'set_multi':
            pass 
         
if __name__ == '__main__':
    recvid = 7
    recvopmsg = "S'set'\np1\n.\tS'hello'\np1\n.\tI5\n.\tI7\n."
    obj = sproxy(recvid)
    obj.parser(recvopmsg)
    
