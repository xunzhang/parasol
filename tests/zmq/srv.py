import zmq
import msgpack
import cPickle

context = zmq.Context()
sock = context.socket(zmq.REP)
sock.bind('tcp://*:7908')
while True:
    msg = sock.recv()
    l = msg.split('\t')
    #optlst = [cPickle.loads(ii) for ii in l]
    optlst = [msgpack.unpackb(ii) for ii in l]
    sock.send('ok')
