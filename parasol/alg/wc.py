# word count using parameter server

class wc(paralg):

    def __init__(self, comm, hosts_dict_lst, nworker, k, input_filename, output):
        paralg.__init__(self, comm, hosts_dict_lst, nworker)
        self.topk = k
        self.filename = input_filename
	self.output = output
	paralg.crt_outfolder(self, self.output)
    
    def check_sum(self):
        pass
    
    def __wc_kernel(self):
        import re
        delimiter = re.compiler('[^-a-zA-Z0-9_]')
	for line in self.linelst:
	    word_lst = [w for w in delimiter.split(line) if w]
	    for word in word_lst:
	        if self.cached_para.get(word):
	            paralg.paralg_inc(self, word, 1)
		else:
		    paralg.paralg_write(self, word, 0)
            paralg.iter_done(self)
        self.comm.barrier()
	# last pull
        self.topk_word_lst = paralg.paralg_read_topk(self, self.topk)
    
    def print_result(self):
        for item in self.topk_word_lst:
	    print item

    def solve(self):
        paralg.loadinput(self, self.filename)
	#linelst = paralg.getlinelst(self)
	self.comm.barrier()
        self.__wc_kernel(linelst)
	self.comm.barrier()
