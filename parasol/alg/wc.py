from parasol.ps import paralg

class wc(paralg):
    
    def __init__(self, comm, hosts_dict_lst, nworker, k, input_filename, output):
        paralg.__init__(self, comm, hosts_dict_lst, nworker)
	self.comm = comm
	self.topk = k
	self.filename = input_filename
	self.output = output
	paralg.crt_outfolder(self, self.output)

    def check_sum(self):
        pass

    def __wc_kernel(self):
        import re
	import time
	delimiter = re.compile('[^-a-zA-Z0-9_]')
	self.cnt = 0
	for line in self.linelst:
	    word_lst = [w for w in delimiter.split(line) if w]
	    for word in word_lst:
	        if word in paralg.get_cache(self):
		    paralg.paralg_inc(self, word, 1)
		else:
		    if paralg.paralg_contains(self, word):
		        paralg.paralg_inc(self, word, 1)
		    else:
		        paralg.paralg_write(self, word, 1)
	    paralg.iter_done(self)
	paralg.sync(self)
	# last pull
	self.topk_word_lst = paralg.paralg_read_topk(self, self.topk)

    def solve(self):
        paralg.loadinput(self, self.filename)
	self.linelst = paralg.getlines(self)
	paralg.sync(self)
	self.__wc_kernel()
	paralg.sync(self)

    def print_result(self):
        if paralg.get_nodeid(self) == 0:
	    for item in self.topk_word_lst:
	        print item
	
