#! /usr/bin/python

import cPickle

class cproxy(Exception):

    def __init__(self, fd):
        self.fd = fd
    
    def push(self, key, val):
        # dump all
        sop = cPickle.dumps('set')
        skey = cPickle.dumps(key)
        sval = cPickle.dumps(val) 
        # joint with '\t'
        sset = sop + '\t' + skey + '\t' + sval + '\t' + 'I' + str(self.fd) + '\n.'
        return sset
        
    def pull(self, key):
        # dump all
        sop = cPickle.dumps('get')
        skey = cPickle.dumps(key)
        # joint with '\t'
        sget = sop + '\t' + skey + '\t' + 'I' + str(self.fd) + '\n.' 
        return sget
        
if __name__ == '__main__':
    # client proxy to call
    obj = cproxy(7)
    st = obj.push('hello', 5)
    st2 = obj.pull('hello')
    #print st
    #print st2
    # server proxy to asw
    l = st.split('\t')
    print [cPickle.loads(i) for i in l]
    l2 = st2.split('\t')
    print [cPickle.loads(i) for i in l2]
