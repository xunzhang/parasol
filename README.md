parasol Overview
================

Parameter Server is a central service for many Machine Learning algorithms(sgd, mf, svd, BFGS...). It not only offers a k-v store service but also abstracts some computational logic. You can get more details in Google's fresh paper:"[Large Scale Distributed Deep Networks](http://www.cs.toronto.edu/~ranzato/publications/DistBeliefNIPS2012_withAppendix.pdf)".

Parasol is a simple Python implementation and it is not fault-tolerant(to be done) thus far.

In using parasol, you can build ML algorithms in yet another model: 'pull when using parameters, push when updating parameters'. It is rather simple model and helps putting huge size of parameters into a global, distributed memory. Since 'more data is always helpful', you can take them and get a better performance in solving large scale problems. 

Parasol also makes communication asynchronously which means workers can do much more calculation in stead.

Have Fun!

Prerequisite
------------
You must install ZemoMQ, mpi4py in advance.

[ZeroMQ](http://zeromq.org) is a high-performance asynchronous messaging library aimed at use in scalable distributed or concurrent applications.

[mpi4py](http://mpi4py.scipy.org) is a Python package(at PyPI) for the Message Passing Interface (MPI) standard.

Installation
------------

``` bash
$ python setup.py install --prefix=/mfs/user/xxx/local
```

Say hi 2 parasol
----------------
Parasol only contains a bsmf algorithm till now. By writing your own alg in parasol you must:

I. write a subclass inherits the 'paralg' class.

II. write a entry for your alg.

III. write a json-config-file which must contains "nworker" which refer to number of workers for calculate and "nserver" which refer to number of servers providing parameter service.

IV. run your alg with 'run_parasol.py':

``` bash
$ ./run_parasol.py --config /xxx/alg_cfg.json python /xxx/entry.py
```

Logo
----
Logo for parasol is really cool I think, you can make it with only one stroke:

``` bash
 (0.5,1) -> (0, 0.5) -> (1,0.5) -> (0.5, 1) -> (0.5, 0.25) -> (0.25, 0.25)
```
