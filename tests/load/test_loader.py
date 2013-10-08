#! /usr/bin/python

from mpi4py import MPI
from parasol.loader.crtblkmtx import ge_blkmtx
from parasol.utils.lineparser import parser_a, parser_b, parser_ussrt

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    lines = ge_blkmtx('a.txt', comm, parser_b) # expect lines
    print lines
    print '------------------------a.txt----------------------------------'
    rm, cm, mtx = ge_blkmtx('a.txt', comm, parser_b, 'fmap', True) # expect mtx
    if rank == 1:
        print rm, cm
        print mtx
    print '-------------------------a.txt---------------------------------'
    rm, cm, mtx = ge_blkmtx('b.txt', comm, parser_b, 'fmap') # expect mtx
    print rm, cm
    print mtx
    print '--------------------------b.txt--------------------------------'
    rm, cm, mtx = ge_blkmtx('c.txt', comm, parser_b, 'fmap', True) # expect mtx
    print rm, cm
    print mtx
    print '---------------------------c.txt-------------------------------'
    rm, cm, mtx = ge_blkmtx('d.txt', comm, parser_ussrt, 'fsmap') # expect mtx
    print rm, cm
    print mtx
    print '----------------------------d.txt------------------------------'
    lines = ge_blkmtx('e.txt', comm, parser_b) # expect lines 
    if rank == 0:
        print lines
    print '----------------------------e.txt------------------------------'
