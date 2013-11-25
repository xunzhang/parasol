import threading

mutex = threading.Lock()

class ld_scheduler(Exception):
    
    def __init__(self, comm, pattern = 'linesplit', mix = False):
        self.comm = comm
	self.leader = 0
	self.blk_sz = 8
	self.pattern = pattern
	self.mix = mix
	self._dim_init()

    def _dim_init(self):
        from decomp import npfactx, npfacty, npfact2d
        np = self.comm.Get_size()
	self.npx, self.npy = npfactx(np)
        if self.pattern == 'fsmap':
	    self.npx, self.npy = npfact2d(np)
	if self.pattern == 'smap':
	    self.npx, self.npy = npfacty(np)

    def h(self, i, j):
        return (hash(i) % self.npx) * self.npy + hash(j) % self.npy

    def schedule_load(self, loads):
        from mpi4py import MPI
        ret = []
	rank = self.comm.Get_rank()
	size = self.comm.Get_size()
        
	if self.pattern == ('linesplit' or 'fvec'):
	    lines = loads[rank]()
	    for line in lines:
	        ret.append(line)
            return ret
        
	cnt = self.blk_sz - 1
	cntctrl = self.blk_sz
	ntasks = size * self.blk_sz
	bnd = ntasks + size - 2
	flag = 0
        
	if rank == self.leader:
	    # load tasks [0, self.blk_sz - 1]
	    for i in xrange(self.blk_sz):
	        lines = loads[i]()
		for line in lines:
		    ret.append(line)
	if rank != self.leader:
	    while 1:
	        if flag == 1: break
		if cnt == ntasks - 1: break
		self.comm.send(rank, self.leader, 2013)
		cnt = self.comm.recv(source = self.leader, tag = 2013)
		flag = self.comm.recv(source = self.leader, tag = 2013)
		if flag == 0:
		    # load
		    lines = loads[cnt]()
		    for line in lines:
		        ret.append(line)
	else:
	    while cntctrl < bnd:
	        cntctrl += 1
		global mutex
		mutex.acquire()
		stat = MPI.Status()
		tmp = self.comm.recv(source = MPI.ANY_SOURCE, tag = MPI.ANY_TAG, status = stat)
		if flag == 0:
		    cnt += 1
		src = stat.Get_source()
		self.comm.send(cnt, src, 2013)
		if cnt == ntasks - 1 and flag == 0:
		    self.comm.send(flag, src, 2013)
		    flag = 1
		else:
		    self.comm.send(flag, src, 2013)
		mutex.release()
        return ret

    def lines_organize(self, lines, parserfunc = (lambda l : l)):
        import re
	import sys
	lineslotslst = [[] for i in xrange(self.comm.Get_size())]
	delimiter = re.compile('[:| ]*')
	for line in lines:
	    stf = parserfunc(line)
	    if stf:
	        # bfs or part of fset case
		if len(stf) == 2:
		    tmp = delimiter.split(stf[1])
		    if len(tmp) == 1:
		        tpl = (stf[0], stf[1], 1)
			lineslotslst[self.h(stf[0], stf[1])].append(tpl)
	            else:
		        tpl = (stf[0], tmp[0], float(tmp[1]))
			lineslotslst[self.h(stf[0], tmp[0])].append(tpl)
		# fset case
		elif self.mix:
		    for item in stf[1:]:
		        tmp = delimiter.split(item)
			if len(tmp) == 1:
			    tpl = (stf[0], item, 1)
			    lineslotslst[self.h(stf[0], item)].append(tpl)
			else:
			    tpl = (stf[0], tmp[0], float(tmp[1]))
			    lineslotslst[self.h(stf[0], tmp[0])].append(tpl)
		# fsv case
		else:
		    if len(stf) != 3:
		        print 'error in lines_organize: fmt of input files not supported!'
			sys.exit(1)
		    tpl = (stf[0], stf[1], float(stf[2]))
		    lineslotslst[self.h(stf[0], stf[1])].append(tpl)
	return lineslotslst

    def exchange(self, slotslst):
        recvobj = self.comm.alltoall(slotslst)
	stf = []
	for item in recvobj:
	    stf += item
	return stf

    def index_mapping(self, slotslst):
        import itertools
        from mpi4py import MPI
	from util import bcastring
	rank = self.comm.Get_rank()

	rowcolor = rank / self.npy
	colcolor = rank % self.npy
	row_comm = self.comm.Split(rowcolor, rank)
	col_comm = self.comm.Split(colcolor, rank)

	rows = [stf[0] for stf in slotslst]
	cols = [stf[1] for stf in slotslst]

	degree = [len(list(grp)) for key, grp in itertools.groupby(sorted(rows))]
	degreemap = {}
	for i in xrange(len(degree)):
	    degreemap[i] = degree[i]
	
	rows = row_comm.allreduce(rows, op = MPI.SUM)
	cols = col_comm.allreduce(cols, op = MPI.SUM)
	#rows = list(set(row_comm.allreduce(rows, op = MPI.SUM)))
	#cols = list(set(col_comm.allreduce(cols, op = MPI.SUM)))
	#rows.sort()
	#cols.sort()

	rowmap = {} 
	rowrevmap = {} 
	colmap = {} 
	colrevmap = {}

	#for ids, ind in enumerate(rows):
	#    rowmap[ids] = ind
	#    rowrevmap[ind] = ids
	#for ids, ind in enumerate(cols):
	#    colmap[ids] = ind
	#    colrevmap[ind] = ids
	#for k in xrange(len(slotslst)):
	#    slotslst[k] = (rowrevmap[slotslst[k][0]], colrevmap[slotslst[k][1]], slotslst[k][2])
        
	newi = []
	newj = []
	rcnt = 0
	ccnt = 0
	for item in rows:
	    if rowrevmap.get(item) == None:
	        rowmap[rcnt] = item
		rowrevmap[item] = rcnt
		rcnt += 1
	    newi.append(rowrevmap[item])
        for item in cols:
	    if colrevmap.get(item) == None:
	        colmap[ccnt] = item
		colrevmap[item] = ccnt
		ccnt += 1
	    newj.append(colrevmap[item])
	for k in xrange(len(slotslst)):
	    slotslst[k] = (rowrevmap[slotslst[k][0]], colrevmap[slotslst[k][1]], slotslst[k][2])

        col_degreemap = {} 
	reducemap = {}
	for triple in slotslst:
	    key = triple[1]
	    if not reducemap.get(key):
	        reducemap[key] = 1
	    else:
	        reducemap[key] += 1

	def cnt_reduce(kvpair):
	    for kv in kvpair.items():
	        key = kv[0]
		if not col_degreemap.get(key):
		    col_degreemap[key] = kv[1]
		else:
		    col_degreemap[key] += kv[1]

	bcastring(reducemap, cnt_reduce, col_comm)

	return slotslst, rowmap, colmap,  degreemap, col_degreemap
