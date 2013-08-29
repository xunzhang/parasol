#! /usr/bin/python
from pykv import pykv

KVNUM = 100

kvpoll_lst = []
for i in xrange(KVNUM):
    kvpoll_lst.append(pykv())

threadnum = 0
