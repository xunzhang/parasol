from mpi4py import MPI
import msgpack
import zmq
import cPickle

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

#context = zmq.Context()
#sock = context.socket(zmq.REQ)
#sock.connect('tcp://balin:7908')

p = [i * 5.312414 for i in xrange(20)]

cnt = 0
for i in xrange(100000):
    context = zmq.Context()
    sock = context.socket(zmq.REQ)
    sock.connect('tcp://dwalin.intra.douban.com:7908')
    print cnt
    cnt += 1
    key = 'p[' + str(i) + ',:]_' + '0'
    #m = cPickle.dumps('push') + '\t' + cPickle.dumps(key) + '\t' + cPickle.dumps(p)
    m = msgpack.packb('push') + '\t' + msgpack.packb(key) + '\t' + msgpack.packb(p)
    sock.send(m)
    ret = sock.recv()
