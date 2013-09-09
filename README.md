parasol Overview
================

Parameter Server is a central service for lots of Machine Learning algorithms(sgd, mf, svd, BFGS...) which not only offer a k-v store service but also some computational logic(asynchronization, cache...). You can get more details in Google's fresh paper "Large Scale Distributed Deep Networks".

Parasol is a simple Python version implementation of Parameter Server, parasol is not fault-tolerant(to be done) thus far.

With parasol you can build your ML algorithm in yet another model: pull when using parameters, push when updating parameters. It is really simple model which helps you to distribute parameters into a distributed service. Since 'more data is usally helpful', you can take them and get a higher level in solving large scale parameters. Parasol makes scalability of learning algorithms following the pace of big data, for example a huge parameters space in deep learning. On the other hand, Parasol makes communication asynchronous which means workers can do much more calculation.

Have Fun!

Prerequisite
------------
You must install ZemoMQ, mpi4py in advance.
ZeroMQ is a high-performance asynchronous messaging library aimed at use in scalable distributed or concurrent applications.
mpi4py is a Python package(@PyPI) for the Message Passing Interface (MPI) standard.

Installation
------------

``` bash
$ python setup.py install --prefix=/mfs/user/xxx/local
```

Say hi 2 Parasol
----------------
Parasol only contains a bsmf algorithm now. By writing your alg in parasol you must:

I. write a subclass inherits the 'paralg' class.

II. write a entry to exec your alg.

III. write a json-config-file which must contains "nworker" which refer to number of workers for calculate and "nserver" which refer to number of servers supply parameter service.

IV. run your alg with run_parasol.py script:

``` bash
$ ./run_parasol.py --config /xxx/alg_cfg.json python /xxx/entry.py
```

Logo
----
Logo for parasol is really simple, you can make it with only one stroke:

``` bash
 (0.5,1) -> (0, 0.5) -> (1,0.5) -> (0.5, 1) -> (0.5, 0.25) -> (0.25, 0.25)
```
