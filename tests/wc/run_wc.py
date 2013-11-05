#! /usr/bin/python

if __name__ == '__main__':
    import json
    from mpi4py import MPI
    from optparse import OptionParser
    from parasol.alg.wc import wc
    from parasol.utils.gethostnames import get_hostnames_dict
    comm = MPI.COMM_WORLD
    
    optpar = OptionParser()
    optpar.add_option('--hostsname', action = 'store', type = 'string', dest = 'hosts_name_st', help = 'hosts name string of paralg servers', metavar = 'dwalin:8888parasolbalin:7777')
    options, args = optpar.parse_args()
    hosts_dict_lst = get_hostnames_dict(options.hosts_name_st)

    cfg_file = '/home/xunzhang/xunzhang/Proj/parasol/config/wc_cfg.json'
    json_obj = json.loads(open(cfg_file).read())
    nsrv = json_obj['nserver']
    nworker = json_obj['nworker']
    k = json_obj['k']
    input_filename = json_obj['input']
    output = json_obj['output']
    
    wc_solver = wc(comm, hosts_dict_lst, nworker, k, input_filename, output)
    wc_solver.solve()
    #wc_solver.write_wc_result()
    wc_solver.print_result()
