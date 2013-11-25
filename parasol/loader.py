from ld_scheduler import ld_scheduler
from partition import files_partition
from util import expand

class loader(Exception):
    
    def __init__(self, filenames, comm, pattern = 'linesplit', parserfunc = (lambda l : l),  mix = False):
        self.filenames = filenames
	self.comm = comm
	self.pattern = pattern
	self.mix = mix
	self.parserfunc = parserfunc
        self.scheduler = ld_scheduler(self.comm, self.pattern, self.mix)

    def load(self):
	fname_lst = expand(self.filenames)
	loads = files_partition(fname_lst, self.comm.Get_size(), self.pattern)
	print 'rank %d load generated' % self.comm.Get_rank()
	linelst = self.scheduler.schedule_load(loads)
	print 'rank %d lines got' % self.comm.Get_rank()
	self.comm.barrier()
	return linelst
    
    def create_graph(self, lines):
        import numpy as np
	from scipy.sparse import coo_matrix
        if self.pattern == 'fvec':
	    rmap = {}
	    dense_mtx = []
	    cnt = 0
	    for line in lines:
	        stf = self.parserfunc(line)
		rmap[cnt] = stf[0]
		dense_mtx.append(stf[1:])
		cnt += 1
	    dense_mtx = np.array(mtx)
	    return dense_mtx, rmap
	else:
	    slotslst = self.scheduler.lines_organize(lines, self.parserfunc)
	    print 'rank %d slotslst generated' % self.comm.Get_rank()
	    self.comm.barrier()
	    slotslst = self.scheduler.exchange(slotslst)
	    print 'rank %d get desirable lines' % self.comm.Get_rank()
	    slotslst, rmap, cmap, dmap, col_dmap = self.scheduler.index_mapping(slotslst)
	    print 'rank %d finish ind_mapping' % self.comm.Get_rank()
	    self.comm.barrier()
	    graph = zip(np.array([i[0] for i in slotslst]), np.array([i[1] for i in slotslst]), np.array([float(i[2]) for i in slotslst]))
	    print 'rank %d create graph' % self.comm.Get_rank()
	    return graph, rmap, cmap, dmap, col_dmap

    def create_matrix(self, lines):
        import numpy as np
	from scipy.sparse import coo_matrix
        if self.pattern == 'fvec':
	    rmap = {}
	    dense_mtx = []
	    cnt = 0
	    for line in lines:
	        stf = self.parserfunc(line)
		rmap[cnt] = stf[0]
		dense_mtx.append(stf[1:])
		cnt += 1
	    dense_mtx = np.array(mtx)
	    return dense_mtx, rmap
	else:
	    slotslst = self.scheduler.lines_organize(lines, self.parserfunc)
	    print 'rank %d slotslst generated' % self.comm.Get_rank()
	    self.comm.barrier()
	    slotslst = self.scheduler.exchange(slotslst)
	    print 'rank %d get desirable lines' % self.comm.Get_rank()
	    slotslst, rmap, cmap, dmap, col_dmap = self.scheduler.index_mapping(slotslst)
	    print 'rank %d finish ind_mapping' % self.comm.Get_rank()
	    self.comm.barrier()
	    mtx = coo_matrix((np.array([i[2] for i in slotslst]), (np.array([i[0] for i in slotslst]), np.array([i[1] for i in slotslst]))))
	    print 'rank %d create matrix' % self.comm.Get_rank()
	    return mtx, rmap, cmap, dmap, col_dmap

if __name__ == '__main__':
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    lder = loader('/home/xunzhang/xunzhang/Data/classification/train140', comm, 'linesplit')
    lines = lder.load()
    print comm.Get_rank(), len(lines)

    f = lambda line: line.strip('\n').split('\t')
    lder2 = loader('/home/xunzhang/xunzhang/Proj/parasol/tests/load/b.txt', comm, 'fsmap', f)
    lines = lder2.load()
    mtx, rmap, cmap, dmap, col_dmap = lder2.create_matrix(lines)
    if comm.Get_rank() == 2:
        print 'mtx', mtx
        print 'rmap', rmap
        print 'cmap', cmap
        print 'dmap', dmap
        print 'col_map', col_dmap
