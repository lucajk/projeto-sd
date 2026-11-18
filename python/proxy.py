import zmq

context = zmq.Context()

# servidores publicam aqui
sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

# bots se inscrevem aqui
pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

print("proxy rodando", flush=True)
zmq.proxy(pub, sub)

pub.close()
sub.close()
context.close()
