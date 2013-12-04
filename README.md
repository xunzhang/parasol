Parasol Overview
================

Parasol is a lightweight distributed computational framework designed for many machine learning problems: SVD, MF(BFGS, sgd, als, cg), LDA, Lasso.... 

Firstly, parasol split both massive dataset and massive parameter space. Unlike Mapreduce-like systems, parasol give a simple communication model, which allows you to work with a global, distributed key-value storage called parameter server. 

In using parasol, you can build algorithms following this rule: 'pull parameters before learning | push parameter's updates after learning'. It is rather a simple model(compared to MPI) and is almost painless from serial to parallel.

Secondly, parasol try to solve 'the last-reducer problem' of iterative tasks. We use bounded staleness and find a sweet spot between 'improve-iter' curve and 'iter-sec' curve. A global scheduler take charge of asynchronous working. This method is already proved to be a generalization of BSP/Pregel(by CMU).

Parasol is a Python implementation and originally motivated by Jeff Dean's talk @Stanford in 2013. You can get more details in his paper: "[Large Scale Distributed Deep Networks](http://static.googleusercontent.com/media/research.google.com/en//archive/large_deep_networks_nips2012.pdf)".

Since 'more data is always valuable', you can handle them and get a better performance using parasol. 

Have Fun!

Prerequisite
------------
You must install ZemoMQ, Mpi4py in advance.

[ZeroMQ](http://zeromq.org) is a high-performance asynchronous messaging library aimed at use in scalable distributed or concurrent applications.

[Mpi4py](http://mpi4py.scipy.org) is a Python package(at PyPI) for the Message Passing Interface (MPI) standard.

Installation
------------

``` bash
$ python setup.py install --prefix=xxx
```

Say hi 2 parasol
----------------
Parasol only contains limited algorithms till now. (Logistic Regression, Matrix Factorization, Word Count)

By writing your own alg in parasol you must:

I. write a subclass inherits the 'paralg' class.

II. write a entry for your alg.

III. write a json-config-file which must contains "nworker" which refer to number of workers for calculate and "nserver" which refer to number of servers providing parameter service.

IV. run your alg with 'run_parasol.py':

``` bash
$ ./run_parasol.py --config /xxx/alg_cfg.json python /xxx/entry.py
```

Logo
----
Logo for parasol is really cool, you can make it with only one stroke:

``` bash
 (0.5,1) -> (0, 0.5) -> (1,0.5) -> (0.5, 1) -> (0.5, 0.25) -> (0.25, 0.25)
```

Whisper
-------
Since Python is slow, I am now rewriting a C++ version which is called [Paracel](http://code.dapps.douban.com/paracel).

If you are using parasol, let me know.

Any bugs and related problems, just [ping me]<wuhong@douban.com>.
