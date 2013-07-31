#! /usr/bin/python
#
# wuhong<wuhong@douban.com>
#
# sgd demo 
#

import memcache 
from pykv import pykv

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
                theta[k] = kvtheta.get('theta' + str(k))
            delta = alpha * loss_func_gra(theta, samplex[it], labely[it]) * samplex[it][i]
            theta[i] = delta
            st = 'theta' + str(i)
            # push after calc
            kvtheta.set(st, theta[i])
            print theta
        print 'iter'
    return theta

if __name__ == '__main__':
    samplex = [[1, 1, 1, 1], [1, 2, 2, 2], [1, 3, 3, 3]]
    labely = [1, 2, 3]
    theta = [0, 0.3, 0.3, 0.3]
    #mc = memcache.Client(['127.0.0.1:7900'], debug = 0)
    #mc.set_multi({'theta0' : 0, 'theta1' : 0.3, 'theta2' : 0.3, 'theta3' : 0.3})
    #print mc.get_multi(['theta0', 'theta1', 'theta2', 'theta3']).values()
    kvtheta = pykv()
    #kvtheta.set('theta0', 0)
    #kvtheta.set('theta1', 0.3)
    #kvtheta.set('theta2', 0.3)
    #kvtheta.set('theta3', 0.3)
    kvtheta.set_multi({'theta0' : 0, 'theta1' : 0.3, 'theta2' : 0.3, 'theta3' : 0.3})
    #print kvtheta.get_multi(['theta0', 'theta1', 'theta2', 'theta3'])
    sgd(3, 4, samplex, labely, theta)
    kvtheta.finalize() 
