#! /usr/bin/python
# ./run_parasol -p 12 -w 32 -e /home/xunzhang/xunzhang/Proj/parasol/tests/mf/run_bsmf.py
# ./run_parasol --parasrvnum 12 --workernum 32 --entry /home/xunzhang/xunzhang/Proj/parasol/tests/mf/run_bsmf.py

import socket
import subprocess
from optparse import OptionParser

if __name__ == '__main__':
    optpar = OptionParser()
    optpar.add_option('-p', '--parasrvnum', action = 'store', type = 'int', default = 12, dest = 'parasrv_num', help = 'number of parameter servers', metavar = 12)
    optpar.add_option('-w', '--workernum', action = 'store', type = 'int', default = 32, dest = 'worker_num', help = 'number of workers for cals', metavar = 32)
    optpar.add_option('-e', '--entry', action = 'store', type = 'string', default = 'run_bsmf.py', dest = 'entry', help = 'entry of your alg entry')
    options, args = optpar.parse_args()
    
    start_parasrv_cmd = 'mrun -n ' + str(options.parasrv_num) + ' -p 1 python ./parasol/server/start_srv.py -host ' + socket.gethostname()
    subprocess.Popen(start_parasrv_cmd, shell = True)
    #subprocess.call(start_parasrv_cmd, shell = True)
    
    start_alg_cmd = 'mrun -n ' + str(options.worker_num) + ' python ' + options.entry
    subprocess.Popen(start_alg_cmd, shell = True)

