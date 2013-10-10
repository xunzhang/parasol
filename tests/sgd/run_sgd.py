#! /usr/bin/python

if __name__ == '__main__':
    import json
    from mpi4py import MPI
    from optparse import OptionParser
    from parasol.alg.sgd import sgd
    from parasol.utils.gethostnames import get_hostnames_dict
    comm = MPI.COMM_WORLD

    optpar = OptionParser()
    optpar.add_option('--hostsname', action = 'store', type = 'string', dest = 'hosts_name_st', help = 'hosts name string of parasol servers', metavar = 'dwalin:8888parasolbalin:7777')
    options, args = optpar.parse_args()
    hosts_dict_lst = get_hostnames_dict(options.hosts_name_st)

    cfg_file = '/mfs/user/wuhong/release/parasol/config/sgd_cfg.json'
    json_obj = json.loads(open(cfg_file).read())
    nsrv = json_obj['nserver']
    nworker = json_obj['nworker']
    k = json_obj['k']
    input_filename = json_obj['input']
    output = json_obj['output']

    alpha = json_obj['alpha']
    beta = json_obj['beta']
    rounds = json_obj['rounds'] 
    
    sgd_solver = sgd(comm, hosts_dict_lst, nworker, k, input_filename, output, alpha, beta, rounds)

    sgd_solver.solve()
    #print sgd_solver.theta
    
    esum = sgd_solver.calc_loss()
    if comm.Get_rank() == 0:
        print 'esum is', esum
    
    sgd_solver.write_sgd_result()
