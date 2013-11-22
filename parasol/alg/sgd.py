# serial version 

import numpy as np
from parasol.ps import paralg
from parasol.writer.writer import outputline
#from parasol.utils.parallel import npfactx

class sgd(paralg):
  
    def __init__(self, comm, hosts_dict_lst, nworker, input_filename, output, alpha = 0.002, beta = 0.1, rounds = 3):
    	paralg.__init__(self, comm, hosts_dict_lst)
	self.rank = self.comm.Get_rank()
	#self.a, self.b = npfactx(nworker)
	self.filename = input_filename
	self.output = output

	self.alpha = alpha
	self.beta = beta
	self.rounds = rounds
	# create folder
	paralg.crt_outfolder(self, self.output)

    def loss_func_gra(self, x, theta):
        from math import e
	term = e ** (np.dot(x, theta))
	return term / (1. + term)
 
    def __sgd_kernel(self, debug = False): #sample, label, rounds = 20):
	import random
	from array import array
	if debug:
	    err = array('f', [])
	m, n = self.sample.shape
	#if self.rank == 0:
	self.theta = np.random.random(n)
	paralg.paralg_write(self, 'theta', self.theta)
	z = np.arange(m)
	cnt = 0
	self.theta = 0
	for it in xrange(self.rounds):
	    # shuffle indics
	    random.shuffle(z)
	    # traverse samples
            #self.theta = np.array(paralg.paralg_read(self, 'theta'))
	    for i in z:
	        cnt += 1
		# before calc, pull theta first
		if cnt == 0 or cnt % 100 == 0:
		    self.theta = np.array(paralg.paralg_read(self, 'theta'))
		# update weights
		delta = self.alpha * (self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) * self.sample[i] + self.beta * 2. * self.alpha * self.theta
	        # push delta
		if cnt == 0 or cnt % 100 == 0:
		    paralg.paralg_inc(self, 'theta', delta)
		self.theta = self.theta + delta
            #paralg.paralg_inc(self, 'theta', delta)
            if debug:
		err.append(sum([(self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) ** 2 for i in range(m)]))
	self.comm.barrier()
	self.theta = np.array(paralg.paralg_read(self, 'theta'))
	if debug:
	    return err

    def parser_local(self, linelst):
       import numpy as np
       print 'debug', self.rank, len(linelst)
       a = []
       b = []
       for line in linelst: 
           tmp = [float(item) for item in line.strip('\n').split(',')]
           a.append(tmp[0:-1])
           b.append(tmp[-1])
       a = np.array(a)
       a = np.hstack( (np.ones( (a.shape[0], 1.) ), a) )
       return a, np.array(b)

    def solve(self):
	import time
	#from sklearn import datasets
	import matplotlib.pyplot as plt
    	from parasol.utils.lineparser import parser_b
	paralg.loadinput(self, self.filename)
        self.sample, self.label = self.parser_local(self.linelst)
	self.comm.barrier()
        s = time.time()
	#debug_flag = True
	debug_flag = False
        if debug_flag:
	    err = self.__sgd_kernel(debug_flag)
	    if self.rank == 0:
	        print err
	    plt.plot(err, linewidth = 2)
	    plt.xlabel('Training example', fontsize = 20)
	    plt.ylabel('Error', fontsize = 20)
	    plt.show()
	else:
	    self.__sgd_kernel(debug_flag)
	f = time.time()
	print 'rank ', self.rank, ' kernel time ', f - s
	self.comm.barrier()

    def calc_loss(self):
        from mpi4py import MPI
	m, n = self.sample.shape
	esum = sum( [(self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) ** 2 for i in range(m)] )
	return self.comm.allreduce(esum, op = MPI.SUM) / self.comm.allreduce(m, op = MPI.SUM)

    def write_sgd_result(self):
	if self.comm.Get_rank() == 0:
	    outputline(self.output + 'result', self.theta, '\t')
