#! /usr/bin/python

from mpi4py import MPI
from parasol.loader.crtblkmtx import ge_blkmtx
from parasol.utils.lineparser import parser_a, parser_b, parser_c, parser_d, parser_e, parser_ussrt

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    lines = ge_blkmtx('a.txt', comm, parser_b('\t')) # expect lines
    print lines
    print '------------------------a.txt----------------------------------'
    rm, cm, mtx = ge_blkmtx('a.txt', comm, parser_b('\t'), 'fmap', True) # expect mtx
    if rank == 1:
        print rm, cm
        print mtx
    	print '-------------------------a.txt---------------------------------'
    rm, cm, mtx = ge_blkmtx('b.txt', comm, parser_b('\t'), 'fmap') # expect mtx
    if rank == 1:
        print rm, cm
        print mtx
    	print '--------------------------b.txt--------------------------------'
    rm, cm, mtx = ge_blkmtx('c.txt', comm, parser_b('\t'), 'fmap', True) # expect mtx
    if rank == 1:
        print rm, cm
        print mtx
    	print '---------------------------c.txt-------------------------------'
    #rm, cm, mtx = ge_blkmtx('d.txt', comm, parser_ussrt, 'fsmap') # expect mtx
    #print rm, cm
    #print mtx
    #print '----------------------------d.txt------------------------------'
    rm, cm, mtx = ge_blkmtx('a2.txt', comm, parser_c('\t', '|'), 'fmap', True)
    if rank == 1:
        print rm, cm
        print mtx
    	print '----------------------------a2.txt------------------------------'
    rm, cm, mtx = ge_blkmtx('f.txt', comm, parser_c('\t', '|'), 'fmap', True)
    if rank == 1:
        print rm, cm
        print mtx
    	print '----------------------------f.txt------------------------------'
    rm, cm, mtx = ge_blkmtx('h.txt', comm, parser_b('\t'), 'fmap', True)
    if rank == 1:
        print rm, cm
    	print mtx
    	print '----------------------------h.txt------------------------------'
    rm, cm, mtx = ge_blkmtx('d2.txt', comm, parser_b('\t'), 'fsmap')
    if rank == 1:
    	print rm, cm
    	print mtx
    	print '----------------------------d2.txt------------------------------'
    rm, mtx = ge_blkmtx('g.txt', comm, parser_e(' ', '|'), 'fvec')
    if rank == 1:
    	print rm 
    	print mtx
    	print '----------------------------g.txt------------------------------'
    rm, mtx = ge_blkmtx('g2.txt', comm, parser_e(' ', '|'), 'fvec')
    if rank == 1:
    	print rm
    	print mtx
    	print '----------------------------g2.txt------------------------------'
    #rm, mtx = ge_blkmtx('e.txt', comm, parser_d('\t'), 'fvec') 
    #print rm
    #print mtx
    #print '----------------------------e.txt------------------------------'
