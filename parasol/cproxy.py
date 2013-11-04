#! /usr/bin/python

#import cPickle
import msgpack as mp

class cproxy(Exception):

    def __init__(self, fd):
        self.fd = fd
        #self.sfd = ''
        #self.sfd = self.sfd.join(['I', str(self.fd), '\n.'])
    
    def glue(self, opname, *args):
        #sop = cPickle.dumps(opname)
        sop = mp.packb(opname)
        spara = ''
        for arg in args:
            #spara += cPickle.dumps(arg)
            spara += mp.packb(arg)
            # joint with '\t'
            spara += 'parasol'
        result = ''
        #result = result.join([sop, '\t', spara, self.sfd])
        result = result.join([sop, 'parasol', spara, mp.packb('0')])
        return result
        
    def push(self, key, val):
        return self.glue('push', key, val)
    
    def push_multi(self, kvdict):
        return self.glue('push_multi', kvdict)
         
    def pull(self, key):
        return self.glue('pull', key)

    def pull_multi(self, keylst):
        return self.glue('pull_multi', keylst)
         
    def inc(self, key, delta):
        return self.glue('inc', key, delta)
        
    def pushs(self, key):
        return self.glue('pushs', key)
        
    def pulls(self, key, val, uniq):
        return self.glue('pulls', key, val, uniq)
        
    def remove(self, key):
        return self.glue('remove', key)
        
    def clear(self):
        return self.glue('clear') 

    def pullall(self):
        return self.glue('pullall')

    #def pull1by1(self):
    #    return self.glue('pull1by1')

if __name__ == '__main__':
    import numpy as np
    # client proxy to call
    obj = cproxy(7)
    st = obj.push('hello', 5)
    st2 = obj.pull('hello')
    st3 = obj.glue('push', 'p[4,:]', [0.52937447602997167, 0.21141945059150646])
    #print st
    #print st3
    #print [cPickle.loads(i) for i in st3.split('\t')]
    print [mp.unpackb(i) for i in st3.split('\t')]
    # server proxy to asw
    l = st.split('\t')
    #print [cPickle.loads(i) for i in l]
    print [mp.unpackb(i) for i in l]
    l2 = st2.split('\t')
    #print [cPickle.loads(i) for i in l2]
    print [mp.unpackb(i) for i in l2]
    p = np.random.rand(5, 2)
    st4 =  obj.push_multi({'p[0,:]' : list(p[0, :]), 'p[1,:]' : list(p[1, :]), 'p[2,:]' : list(p[2, :]), 'p[3,:]' : list(p[3, :]), 'p[4,:]' : list(p[4, :])})
    ll = st4.split('\t')
    #print ll
    #print [cPickle.loads(i) for i in ll]
    #print [mp.unpackb(i) for i in ll]
