# thrift client for python

version `0.0.1`


fork from `https://github.com/amuraru/thrift-connection-pool.git`


## usage

```py

import sys

sys.path.append('gen-py')
from pingpong import PingService
from pingpong.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import client


def test():
    cl = client.Client(iface_cls=PingService.Client, host='localhost', port=6000, pool_size=3, retries=3,
                       network_timeout=3000,
                       transport_cls=TSocket.TSocket, transport_wrapper_cls=TTransport.TFramedTransport,
                       protocol_cls=TBinaryProtocol.TBinaryProtocol,
                       debug=True)
    for i in xrange(100):
        print(cl.ping())
    cl.close()
```
