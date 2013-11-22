from math import e
from array import array
import numpy as np

def h(x,theta):
    tmp = np.dot(x, theta)
    tmp2 = 1. / (1. + e ** tmp)
    return e ** (tmp) * tmp2

def log_reg_sgd(x, y, alpha, max_iter = 100, debug = False):
    if debug: err = array('f', [])
    m,n = x.shape
    theta = np.random.random(n)
    z = np.arange(m)
    for t in xrange(max_iter):
        #shuffle indices prior to searching
        z = np.random.permutation(z)
        #for each training example
        for i in z:
            theta = theta + alpha * (y[i] - h(x[i], theta)) * x[i]
            #if debug:err.append(sum([(y[i]-h(x[i],theta))**2 for i in range(m)]))
        if debug: err.append(sum([(y[i] - h(x[i], theta)) ** 2 for i in range(m)]))
    if debug: return theta,err
    return theta


def log_reg_regularized_sgd(x, y, alpha, beta = 0.1, max_iter = 100, debug = False):
    if debug: err = array('f', [])
    m,n = x.shape
    theta = np.random.random(n)
    z = np.arange(m)
    for t in xrange(max_iter):
        #shuffle indices prior to searching
        z = np.random.permutation(z)
        #for each training example
        for i in z:
            theta = theta + alpha * (y[i] - h(x[i], theta)) * x[i] - beta * 2. * alpha * theta
            #if debug:err.append(sum([(y[i]-h(x[i],theta))**2 for i in range(m)]))
        if debug: err.append(sum([(y[i] - h(x[i], theta)) ** 2 for i in range(m)]) / m)
    if debug: return theta,err
    return theta
