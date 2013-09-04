#! /usr/bin/python

from distutils.core import setup

setup(name = 'parasol',
    version = '0.1',
    description = 'Parameter Server for ML algs',
    author = 'Hong Wu, Changsheng Jiang',
    author_email = 'wuhong@douban.com, xunzhangthu@gmail.com',
    packages = ['parasol', 'parasol.server', 'parasol.loader', 'parasol.utils', 'parasol.writer', 'parasol.alg'],
)
