import msgpack as mp

class cproxy(Exception):

    def __init__(self, fd):
        self.fd = fd
    
    def glue(self, opname, *args):
        sop = mp.packb(opname)
        spara = ''
        for arg in args:
            spara += mp.packb(arg)
            spara += 'parasol'
        result = ''
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
        return self.glue('update', key, delta)
        
    def pushs(self, key):
        return self.glue('pushs', key)
        
    def pulls(self, key, val, uniq):
        return self.glue('pulls', key, val, uniq)
        
    def remove(self, key):
        return self.glue('remove', key)
        
    def clear(self):
        return self.glue('clear')        
