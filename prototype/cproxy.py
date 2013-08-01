#! /usr/bin/python

import cPickle

class cproxy(Exception):

    def __init__(self, fd):
        self.fd = fd
        self.sfd = ''
        self.sfd = self.sfd.join(['I', str(self.fd), '\n.'])
    
    def glue(self, opname, *args):
        # dump all
        sop = cPickle.dumps(opname)
        spara = ''
        for arg in args:
            spara += cPickle.dumps(arg)
            # joint with '\t'
            spara += '\t'
        result = ''
        result = result.join([sop, '\t', spara, self.sfd])
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
