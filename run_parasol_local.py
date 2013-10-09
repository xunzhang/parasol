#! /usr/bin/python
# ./run_parasol_local.py -p 1 -w 8 python ./tests/mf/run_bsmf.py
# ./run_parasol_local.py --config ./config/bsmf_cfg.json python ./tests/mf/run_bsmf.py

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
    #optpar.add_option('-e', '--entry', action = 'store', type = 'string', dest = 'entry', help = 'entry of your alg entry')
    optpar.add_option('-c', '--config', action = 'store', type = 'string', dest = 'config', help = 'config file for entry script')
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

    start_parasrv_cmd = 'python ./parasol/server/start_srv.py --hostname ' + socket.gethostname() 
    print start_parasrv_cmd
    subprocess.Popen(start_parasrv_cmd, shell = True)

    hosts_dict_st = get_hostnames_st(nsrv)
    
    entry_cmd = ''
    if args:
        entry_cmd = ' '.join(args)
    start_alg_cmd = 'mpirun -n ' + str(nworker) + ' ' + entry_cmd + ' --hostsname ' + hosts_dict_st
    print start_alg_cmd
    subprocess.call(start_alg_cmd, shell = True)
    #subprocess.Popen(start_alg_cmd, shell = True)

