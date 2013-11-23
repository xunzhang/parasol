import matplotlib.pyplot as plt
import numpy as np
import sklearn
from sklearn import metrics, datasets
from scipy.stats import linregress
from sgd import log_reg_sgd, h, log_reg_regularized_sgd

def load(f):
    x = []
    y = []
    for line in f:
        arr = [float(i) for i in line.strip('\n').split(',')]
	x.append(arr[0:-1])
	y.append(arr[-1])
    x = np.array(x)
    x = np.hstack( (np.ones( (x.shape[0], 1.) ), x) )
    y = np.array(y)
    return x, y

def cal_loss(theta, x, y):
    loss = 0.
    for i in xrange(len(y)):
        loss += (h(x[i], theta) - y[i]) ** 2
    return loss / len(y)

def test_sgd():
    import time
    f = file('/home/xunzhang/xunzhang/Data/classification/000.csv')
    x, y = load(f)
   
    err = ''
    #theta = log_reg_sgd(x, y, 0.002, max_iter = 2)
    starttime = time.time()  
    theta, err = log_reg_regularized_sgd(x, y, 0.01, 0.001, max_iter = 10, debug = True)
    #theta = log_reg_regularized_sgd(x, y, 0.01, 0.001, max_iter = 8)
    endtime = time.time()  
    print 'kernel time', (endtime - starttime) 
     
    # print train convergency condition
    if err:
        print 'train mean loss:', err[-1]
        plt.plot(err, linewidth=2)
        plt.xlabel('Training example', fontsize=20)
        plt.ylabel('Error', fontsize=20)
        plt.show()
    
    # calc pred mean loss
    f2 = file('/home/xunzhang/xunzhang/Data/classification/pred10k')
    x_t, y_t = load(f2)
    print 'mean loss', cal_loss(theta, x_t, y_t)

if __name__=="__main__":
    test_sgd()
