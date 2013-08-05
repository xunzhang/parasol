# wuhong<wuhong@douban.com>
#
# sgd demo 
#

import sys
sys.path.append('../prototype/')
import memcache 
from pykv import pykv
from clt import kv

try:
    import numpy as np
except:
    print 'Please install numpy first.'
    exit(0)

def loss_func_gra(theta, sample, label):
    return (np.array(theta).dot(np.array(sample)) - label)

def sgd(xcnt, dimt, samplex, labely, theta, alpha = 0.2, conv = 0.0001):
    for it in xrange(xcnt):
        for i in xrange(dimt):
            # pull before calc
            for k in xrange(dimt):
                theta[k] = float(kvtheta.pull('theta' + str(k)))
            delta = alpha * loss_func_gra(theta, samplex[it], labely[it]) * samplex[it][i]
            theta[i] = delta
            st = 'theta' + str(i)
            # push after calc
            kvtheta.push(st, theta[i])
            print theta
        print 'iter'
    return theta

if __name__ == '__main__':
    samplex = [[1, 1, 1, 1], [1, 2, 2, 2], [1, 3, 3, 3]]
    labely = [1, 2, 3]
    theta = [0, 0.3, 0.3, 0.3]
    kvtheta = kv('localhost', 7900)
    kvtheta.push_multi({'theta0' : 0, 'theta1' : 0.3, 'theta2' : 0.3, 'theta3' : 0.3})
    
    # procs 0
    sgd(3, 4, samplex, labely, theta)
    # procs 1
    #sgd(3, 2, samplex, labely, theta)
