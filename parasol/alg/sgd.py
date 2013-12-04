# serial version 

import numpy as np
from parasol.ps import paralg
from parasol.writer.writer import outputline
#from parasol.utils.parallel import npfactx

def accumulator(a, b):
    return [a[i] + b[i] for i in xrange(len(b))]

class sgd(paralg):
  
    def __init__(self, comm, hosts_dict_lst, nworker, input_filename, output, alpha = 0.002, beta = 0.1, rounds = 3, limit_s = 1):
    	paralg.__init__(self, comm, hosts_dict_lst, nworker, rounds, limit_s)
	self.filename = input_filename
	self.output = output
	self.alpha = alpha
	self.beta = beta
	self.rounds = rounds
	self.nodeid = paralg.get_nodeid(self)
	# create folder
	paralg.crt_outfolder(self, self.output)

    def accumulator(self, a, b):
        return [a[i] + b[i] for i in xrange(len(b))]

    def loss_func_gra(self, x, theta):
        from math import e
	term  = e ** (np.dot(x, theta))
	return term / (1. + term)
 
    def __sgd_kernel(self, debug = False): #sample, label, rounds = 20):
	import random
	from array import array
	from mpi4py import MPI
	import time
	if debug:
	    err = array('f', [])
	m, n = self.sample.shape
	#if self.nodeid == 0:
	self.theta = np.random.random(n)
	paralg.paralg_write(self, 'theta', self.theta)
	z = np.arange(m)
	print 'nodeid: ', self.nodeid, ' | data size is: ', m
	total_datasz = self.comm.allreduce(m, op = MPI.SUM)
	min_datasz = self.comm.allreduce(m, op = MPI.MIN)
	#print 'debug', paralg.paralg_read_all(self)
	#self.theta = 0
	for it in xrange(self.rounds):
	    cnt = 0
	    # shuffle indics
	    random.shuffle(z)
	    # traverse samples
            #self.theta = np.array(paralg.paralg_read(self, 'theta'))
	    for i in z:
	        #if self.comm.Get_nodeid() == 1:
	        #    time.sleep(0.1)
	        cnt += 1
		# before calc, pull theta first
		#if cnt == 0 or cnt % 100 == 0:
		self.theta = np.array(paralg.paralg_read(self, 'theta'))
		# update weights
		grad = self.label[i] - self.loss_func_gra(self.sample[i], self.theta)
		delta = self.alpha * grad * self.sample[i] - self.beta * 2. * self.alpha * self.theta
	        # push delta
		#if cnt == 0 or cnt % 100 == 0:
		accumulator = lambda a, b: [a[i] + b[i] for i in xrange(len(b))]
		paralg.paralg_inc(self, 'theta', list(delta / self.splits), accumulator)
		#paralg.paralg_inc(self, 'theta', list(delta / self.splits), self.accumulator)
		self.theta = self.theta + delta
            #paralg.paralg_inc(self, 'theta', delta)
                if debug and cnt < min_datasz:
		    local_err = sum([(self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) ** 2 for i in range(m)])
		    fz = self.comm.allreduce(local_err, op = MPI.SUM)
                    err.append(fz / total_datasz)
		paralg.iter_done(self)
	paralg.sync(self)
	self.theta = np.array(paralg.paralg_read(self, 'theta'))
	if debug:
	    return err

    def parser_local(self, linelst):
       import numpy as np
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
	import matplotlib.pyplot as plt
    	from parasol.utils.lineparser import parser_b
	paralg.loadinput(self, self.filename)
	self.linelst = paralg.getlines(self)
        self.sample, self.label = self.parser_local(self.linelst)
	self.splits = paralg.get_decomp(self)
	paralg.sync(self)
        s = time.time()
	#debug_flag = True
	debug_flag = False
        if debug_flag:
	    err = self.__sgd_kernel(debug_flag)
	    if self.nodeid == 0:
	    #    print err
	        print len(err)
	        plt.plot(err, linewidth = 2)
	        plt.xlabel('Training example', fontsize = 20)
	        plt.ylabel('Error', fontsize = 20)
	        plt.show()
	else:
	    self.__sgd_kernel(debug_flag)
	f = time.time()
	print 'nodeid ', self.nodeid, ' kernel time ', f - s
	paralg.sync(self)

    def calc_loss(self):
        from mpi4py import MPI
	m, n = self.sample.shape
	esum = sum( [(self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) ** 2 for i in range(m)] )
	return self.comm.allreduce(esum, op = MPI.SUM) / self.comm.allreduce(m, op = MPI.SUM)

    def write_sgd_result(self):
	if self.nodeid == 0:
	    outputline(self.output + 'result', self.theta, '\t')
