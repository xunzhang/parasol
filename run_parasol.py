#! /usr/bin/python
# ./run_parasol.py -p 12 -w 32 python ./tests/mf/run_bsmf.py
# ./run_parasol.py --config ./config/bsmf_cfg.json python ./tests/mf/run_bsmf.py -m mesos

import os
import sys
import json
import socket
import subprocess
from optparse import OptionParser
from parasol.utils.gethostnames import get_hostnames_st

if __name__ == '__main__':
    optpar = OptionParser()
    optpar.add_option('-p', '--parasrvnum', action = 'store', type = 'int', dest = 'parasrv_num', help = 'number of parameter servers', metavar = 1)
    optpar.add_option('-w', '--workernum', action = 'store', type = 'int', dest = 'worker_num', help = 'number of workers for cals', metavar = 1)
    optpar.add_option('-c', '--config', action = 'store', type = 'string', dest = 'config', help = 'config file for entry script')
    optpar.add_option('-m', '--method', action = 'store', type = 'string', dest = 'method', help = 'running method', metavar = 'local')
    optpar.add_option('--hostfile', action = 'store', type = 'string', dest = 'hostfile', help = 'hostfile for mpirun')
    options, args = optpar.parse_args()
    
    nsrv = 1
    nworker = 1
    if options.parasrv_num and options.worker_num:
         nsrv = options.parasrv_num
         nworker = options.worker_num
    elif options.config:
        tmp = json.loads(open(options.config).read())
        nsrv = tmp.get('nserver')
        if not nsrv:
             print 'config file must contain "nserver"'
             sys.exit(1)
	nworker = tmp.get('nworker')
	if not nworker:
             print 'config file must contain "nworker"'
             sys.exit(1)

    if options.method == 'mesos':
        starter = 'mrun -n '
    elif options.method == 'mpi':
        if options.hostfile:
	    starter = 'mpirun --hostfile ' + options.hostfile + ' -n '
	else:
            starter = 'mpirun --hostfile ~/.mpi/parasolsrv.1 -n '
    else:
        if options.method != 'local':
	    print "method: '" + options.method + "' is not allowed!"
	    sys.exit(1)
        starter = 'mpirun -n '

    start_parasrv_cmd = starter + str(nsrv) + ' python ./parasol/server/start_srv.py --hostname ' + socket.gethostname()
    print start_parasrv_cmd
    procs = subprocess.Popen(start_parasrv_cmd, shell = True, preexec_fn = os.setpgrp)
    hosts_dict_st = get_hostnames_st(nsrv)
    
    entry_cmd = ''
    if args:
        entry_cmd = ' '.join(args)
    start_alg_cmd = starter + str(nworker) + ' ' + entry_cmd + ' --hostsname ' + hosts_dict_st
    print start_alg_cmd
    os.system(start_alg_cmd)
    os.killpg(procs.pid, 9)
