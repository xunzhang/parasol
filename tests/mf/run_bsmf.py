#! /usr/bin/python

if __name__ == '__main__':
    from mpi4py import MPI
    from parasol.alg.bsmf import bsmf
    comm = MPI.COMM_WORLD
    
    path = '/home/xunzhang/xunzhang/Proj/parasol/config/'
    srv_sz = 12
    bsmf_solver = bsmf(comm, srv_sz, path + 'bsmf_cfg.json')
    bsmf_solver.solve()
    esum = bsmf_solver.calc_loss()
    if comm.Get_rank() == 0:
        print 'esum is', esum
    bsmf_solver.write_bsmf_result()
    
