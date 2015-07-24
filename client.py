import inspect

from thrift.transport import TTransport

from thrift.transport import TSocket
from thrift.protocol import TCompactProtocol

from pool import ConnectionPool

"""
    Thrift Client proxying thrift methods defined on `iface_cls`
    through an internal pool of persistent connections to remote server (`host:port`)

    Params:
        iface_cls       : thrift generated Client class
        host            : thrift server hostname
        port            : thirft server port
        async           : socket mode
                          set it to `True` when using this instance in async loops
                          default: False - i.e. sync
        pool_size       : number of maximum connections in pool
                          (default: 100)
        retries         : number of retries in case network errors occur
                          (default: 3)
        network_timeout : thrift socket timeout in millis
                          (default: 0, disabled)
        debug           : Enable thrift calls debugging

"""
class Client(object):
    def __init__(self, iface_cls,
                 host, port,
                 pool_size=ConnectionPool.DEFAULT_POOL_SIZE,
                 retries=3,
                 async=False,
                 network_timeout=ConnectionPool.DEFAULT_NETWORK_TIMEOUT,
                 debug=False,
                 transport_cls=TSocket.TSocket,
                 transport_wrapper_cls=TTransport.TFramedTransport,
                 protocol_cls=TCompactProtocol.TCompactProtocol,
                 ):
        self.host = host
        self.port = port
        self.debug = debug
        self.retries = retries
        self._connection_pool = ConnectionPool(host, port, iface_cls, async=async, size=pool_size,
                                               network_timeout=network_timeout,
                                               transport_cls=transport_cls,
                                               transport_wrapper_cls=transport_wrapper_cls,
                                               protocol_cls=protocol_cls)
        self._iface_cls = iface_cls
        #inject all methods defined in the thrift Iface class
        for m in inspect.getmembers(self._iface_cls, predicate=inspect.ismethod):
            setattr(self, m[0], self.__create_thrift_proxy__(m[0]))

    def close(self):
        self._connection_pool.close()


    def __create_thrift_proxy__(self, methodName):
        def __thrift_proxy(*args):
            return self.__thrift_call__(methodName, *args)
        return __thrift_proxy

    def __thrift_call__(self, method, *args):
        attempts_left = self.retries#self._connection_pool.size + 1
        result = None
        while True:
            conn = self._connection_pool.get_connection()
            try:
                if self.debug:
                    print "Thrift Call:%s Args:%s" % (method, args)
                result = getattr(conn, method)(*args)
            except TTransport.TTransportException as e:
                #broken connection, release it
                self._connection_pool.release_conn(conn)
                if attempts_left > 0:
                    attempts_left -= 1
                    continue
                raise e
            except Exception as e:
                #data exceptions, return connection and don't retry
                self._connection_pool.return_connection(conn)
                raise

            #call completed succesfully, return connection to pool
            self._connection_pool.return_connection(conn)
            return result

