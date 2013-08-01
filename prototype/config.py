#! /usr/bin/python
from pykv import pykv

kvpoll_lst = []
for i in xrange(1000):
    kvpoll_lst.append(pykv())

threadnum = 0
