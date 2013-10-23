from os import kill, system
from redis import StrictRedis, WatchError
from multiprocessing import Process
from Queue import Empty
from msgpack import packb
from time import time, sleep
from ring import RedisRing
import zmq

import logging
import settings

logger = logging.getLogger("HorizonLog")


class Worker(Process):
    """
    The worker processes chunks from the queue and appends
    the latest datapoints to their respective timesteps in Redis.
    """
    def __init__(self, parent_pid, context, canary=False):
        super(Worker, self).__init__()
        self.context = context
        self.ring = RedisRing(settings.REDIS_BACKENDS)
        self.parent_pid = parent_pid
        self.daemon = True
        self.canary = canary

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def in_skip_list(self, metric_name):
        """
        Check if the metric is in SKIP_LIST.
        """
        for to_skip in settings.SKIP_LIST:
            if to_skip in metric_name:
                return True

        return False

    def run(self):
        """
        Called when the process intializes.
        """
        logger.info('started worker')

        FULL_NAMESPACE = settings.FULL_NAMESPACE
        MINI_NAMESPACE = settings.MINI_NAMESPACE
        MAX_RESOLUTION = settings.MAX_RESOLUTION

        self.conn = self.context.socket(zmq.PULL)
        self.conn.connect(
            '{0}:{0}'.format(settings.RELAY_HOST, settings.RELAY_PUBLISH_PORT))
        self.poller = zmq.Poller()
        self.poller.register(self.conn, zmq.POLLIN)

        while 1:
            sockets = dict(self.poller.poll())
            if self.conn in sockets and sockets[self.conn] == zmq.POLLIN:
            # Make sure Redis is up
                try:
                    self.ring.check_connections()
                except:
                    sleep(10)
                    self.ring = RedisRing(settings.REDIS_BACKENDS)
                    continue

                try:
                    # Get a chunk from the queue with a 15 second timeout
                    chunk = self.q.get(True, 15)
                    now = time()

                    for metric in chunk:

                        # Check if we should skip it
                        if self.in_skip_list(metric[0]):
                            continue

                        # Bad data coming in
                        if metric[1][0] < now - MAX_RESOLUTION:
                            continue

                        for ns in [FULL_NAMESPACE, MINI_NAMESPACE]:
                            key = ''.join((ns, metric[0]))
                            self.ring.run('append', key, packb(metric[1]))
                            ukey = ''.join((ns,'unique_metrics.',metric[0]))
                            self.ring.run('sadd', ukey, key)

                except Exception as e:
                    logger.error("worker error: " + str(e))