# Stochastic Gradient Descend

import numpy as np
from parasol.ps import paralg
#from parasol.utils.parallel import npfactx

class sgd(paralg):
  
    def __init__(self, comm, hosts_dict_lst, nworker, k, input_filename, output, alpha = 0.002, beta = 0.1, rounds = 3):
    	paralg.__init__(self, comm, hosts_dict_lst)
	self.rank = self.comm.Get_rank()
	#self.a, self.b = npfactx(nworker)
	self.k = k
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
	if self.rank == 0:
	    self.theta = np.random.rand(n)
	    paralg.paralg_write(self, 'theta', self.theta)
	z = np.arange(m)
	for it in xrange(self.rounds):
	    # shuffle indics
	    random.shuffle(z)
	    # traverse samples
	    for i in z:
		# before calc, pull theta first
		self.theta = np.array(paralg.paralg_read(self, 'theta'))
		# update weights
		delta = self.alpha * (self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) * self.sample[i] + 2. * self.beta * self.alpha * self.theta
	        # push delta
		paralg.paralg_inc(self, 'theta', delta)
		self.theta = self.theta + delta
		if debug:
		    err.append(sum([(self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) ** 2 for i in range(m)]))
	self.comm.barrier()
	self.theta = np.array(paralg.paralg_read(self, 'theta'))
	if debug:
	    return err

    def solve(self):
	from sklearn import datasets
    	#from parasol.utils.lineparser import parser_b
	#paralg.loadinput(self, self.filename, parser_b('\t'))
	self.sample, self.label = datasets.make_classification(100, self.k)
	self.sample = np.hstack((np.ones((self.sample.shape[0], 1)), self.sample))	
	self.comm.barrier()
	err = self.__sgd_kernel(True)
	import matplotlib.pyplot as plt
	print err
	plt.plot(err, linewidth = 2)
	plt.xlabel('Training example', fontsize = 20)
	plt.ylabel('Error', fontsize = 20)
	plt.show()
	self.comm.barrier()
