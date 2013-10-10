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
 
    def __sgd_kernel(self): #sample, label, rounds = 20):
	import random
	m, n = self.sample.shape
	self.theta = np.random.rand(n)
	z = np.arange(m)
	for it in xrange(self.rounds):
	    # shuffle indics
	    random.shuffle(z)
	    # traverse samples
	    for i in z:
		# before calc, pull theta first
		# update weights
		delta = self.alpha * (self.label[i] - self.loss_func_gra(self.sample[i], self.theta)) * self.sample[i] + 2. * self.beta * self.alpha * self.theta
	        # push delta
		self.theta += delta
		
    def solve(self):
	from sklearn import datasets
    	#from parasol.utils.lineparser import parser_b
	#paralg.loadinput(self, self.filename, parser_b('\t'))
	self.sample, self.label = datasets.make_classification(100, self.k)
	self.sample = np.hstack((np.ones((self.sample.shape[0], 1)), self.sample))	
	self.comm.barrier()
	self.__sgd_kernel()
	self.comm.barrier()
