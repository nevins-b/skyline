import zmq

import socket
from os import kill, getpid
from Queue import Full
from multiprocessing import Process
from struct import Struct, unpack
from msgpack import unpackb
from cPickle import loads
import re

class Relay(Process):
    """
    The listener is responsible for listening on a port.
    """
    def __init__(self, parent_pid):
        super(Relay, self).__init__()
        try:
            self.ip = settings.RELAY_IP
        except AttributeError:
            # Default for backwards compatibility
            self.ip = socket.gethostname()
        self.parent_pid = parent_pid
        self.port = settings.get('RELAY_LISTEN_PORT', 2702)
        self.queue_port = settings.get('RELAY_PUBLISH_PORT', 2703)
        self.daemon = True
        self.type = settings.get('RELAY_TYPE', 'pickle')
        self.key = settings.get('ACCESS_KEY', '')
        self._connect()

    def _connect(self):
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUSH)
        self.publisher.bind(self.queue_port)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def gen_unpickle(self, infile):
        """
        Generate a pickle from a stream
        """
        try:
            bunch = loads(infile)
            yield bunch
        except EOFError:
            return

    def read_all(self, sock, n):
        """
        Read n bytes from a stream
        """
        data = ''
        while n > 0:
            buf = sock.recv(n)
            n -= len(buf)
            data += buf
        return data

    def white_list_rewrite(self, metric):
        if not metric[0].starts with self.key:
            return None
        else:
            return metric[0] = metric[0].replace('{0}.'.format(self.key ,'')

    def listen_pickle(self):
        """
        Listen for pickles over tcp
        """
        while 1:
            try:
                # Set up the TCP listening socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.ip, self.port))
                s.setblocking(1)
                s.listen(5)
                logger.info('listening over tcp for pickles on %s' % self.port)

                (conn, address) = s.accept()
                logger.info('connection from %s:%s' % (address[0], self.port))

<<<<<<< HEAD:src/relay/relay.py
=======
                chunk = []
>>>>>>> bcb25d59fa66f2f6261386b406941623c10ebe49:src/horizon/listen.py
                while 1:
                    self.check_if_parent_is_alive()
                    try:
                        length = Struct('!I').unpack(self.read_all(conn, 4))
                        body = self.read_all(conn, length[0])

                        # Iterate and chunk each individual datapoint
                        for bunch in self.gen_unpickle(body):
                            for metric in bunch:
                                metric = white_list_rewrite(metric)
                                if metric:
                                    self.publisher.send(metric)

                    except Exception as e:
                        logger.info(e)
                        logger.info('incoming connection dropped, attempting to reconnect')
                        break

            except Exception as e:
                logger.info('can\'t connect to socket: ' + str(e))
                break

    def listen_udp(self):
        """
        Listen over udp for MessagePack strings
        """
        while 1:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.bind((self.ip, self.port))
                logger.info('listening over udp for messagepack on %s' % self.port)

                while 1:
                    self.check_if_parent_is_alive()
                    data, addr = s.recvfrom(1024)
                    metric = unpackb(data)
                    metric = white_list_rewrite(metric)
                        if metric:
                            self.publisher.send(metric)

            except Exception as e:
                logger.info('can\'t connect to socket: ' + str(e))
                break

    def run(self):
        """
        Called when process intializes.
        """
        logger.info('started listener')

        if self.type == 'pickle':
<<<<<<< HEAD:src/relay/relay.py
                self.listen_pickle()
=======
            self.listen_pickle()
>>>>>>> bcb25d59fa66f2f6261386b406941623c10ebe49:src/horizon/listen.py
        elif self.type == 'udp':
            self.listen_udp()
        else:
            logging.error('unknown listener format')
