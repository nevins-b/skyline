from os import kill
from ring import RedisRing
from multiprocessing import Process
from threading import Thread
from msgpack import Unpacker, packb
from types import TupleType
from time import time, sleep
import socket

import logging
import settings

logger = logging.getLogger("HorizonLog")


class Roomba(Thread):
    """
    The Roomba is responsible for deleting keys older than DURATION.
    """
    def __init__(self, parent_pid):
        super(Roomba, self).__init__()
        self.ring = RedisRing(settings.REDIS_BACKENDS)
        self.daemon = True
        self.parent_pid = parent_pid

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def vacuum(self, i, namespace, duration):
        """
        Trim metrics that are older than settings.FULL_DURATION and
        purge old metrics.
        """
        process_key = '.'.join(['skyline','roomba', str(namespace)])
        hostname = socket.gethostname()
        if self.ring.run('get', process_key) != hostname:
            return
        else:
            self.ring.run('set', process_key, hostname)
            self.ring.run('expire', process_key, 600)

        begin = time()
        # Discover assigned metrics
        unique_metrics = list(self.ring.run('smembers', namespace + 'unique_metrics'))
        keys_per_processor = len(unique_metrics) / settings.ROOMBA_PROCESSES
        assigned_max = i * keys_per_processor
        assigned_min = assigned_max - keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]

        euthanized = 0
        blocked = 0
        for i in xrange(len(assigned_metrics)):
            self.check_if_parent_is_alive()

            now = time()
            key = assigned_metrics[i]

            try:
                # WATCH the key
                pipe.watch(key)

                # Everything below NEEDS to happen before another datapoint
                # comes in. If your data has a very small resolution (<.1s),
                # this technique may not suit you.
                raw_series = self.ring.run('get', key)
                unpacker = Unpacker(use_list = False)
                unpacker.feed(raw_series)
                timeseries = sorted([unpacked for unpacked in unpacker])

                # Put pipe back in multi mode
                pipe.multi()

                # There's one value. Purge if it's too old
                try:
                    if not isinstance(timeseries[0], TupleType):
                        if timeseries[0] < now - duration:
                            self.ring.run('delete', key)
                            self.ring.run('srem', namespace + 'unique_metrics', key)
                            euthanized += 1
                        continue
                except IndexError:
                    continue

                # Check if the last value is too old and purge
                if timeseries[-1][0] < now - duration:
                    self.ring.run('delete', key)
                    self.ring.run('srem', namespace + 'unique_metrics', key)
                    euthanized += 1
                    continue

                # Remove old datapoints and duplicates from timeseries
                temp = set()
                temp_add = temp.add
                delta = now - duration
                trimmed = [
<<<<<<< HEAD
                            tuple for tuple in timeseries
                            if tuple[0] > delta
                            and tuple[0] not in temp
                            and not temp_add(tuple[0])
                        ]
=======
                    tuple for tuple in timeseries
                    if tuple[0] > delta
                    and tuple[0] not in temp
                    and not temp_add(tuple[0])
                ]
>>>>>>> bcb25d59fa66f2f6261386b406941623c10ebe49

                # Purge if everything was deleted, set key otherwise
                if len(trimmed) > 0:
                    # Serialize and turn key back into not-an-array
                    btrimmed = packb(trimmed)
                    if len(trimmed) <= 15:
                        value = btrimmed[1:]
                    elif len(trimmed) <= 65535:
                        value = btrimmed[3:]
                    else:
                        value = btrimmed[5:]
                    pipe.set(key, value)
                else:
                    self.ring.run('delete', key)
                    self.ring.run('srem', namespace + 'unique_metrics', key)
                    euthanized += 1

                pipe.execute()

            except WatchError:
                blocked += 1
                assigned_metrics.append(key)
            except Exception as e:
                # If something bad happens, zap the key and hope it goes away
                self.ring.run('delete', key)
                self.ring.run('srem', namespace + 'unique_metrics', key)
                euthanized += 1
                logger.info(e)
                logger.info("Euthanizing " + key)
            finally:
                pipe.reset()

        logger.info('operated on %s in %f seconds' % (namespace, time() - begin))
        logger.info('%s keyspace is %d' % (namespace, (len(assigned_metrics) - euthanized)))
        logger.info('blocked %d times' % blocked)
        logger.info('euthanized %d geriatric keys' % euthanized)

        if (time() - begin < 30):
            logger.info('sleeping due to low run time...')
            sleep(10)

    def run(self):
        """
        Called when process initializes.
        """
        logger.info('started roomba')

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.ring.check_connections()
            except:
                sleep(10)
                self.ring = RedisRing(settings.REDIS_BACKENDS)
                continue

            # Spawn processes
            pids = []
            for i in range(1, settings.ROOMBA_PROCESSES + 1):
                p = Process(
                    target=self.vacuum,
                    args=(i,
                        settings.MINI_NAMESPACE,
                        settings.MINI_DURATION + settings.ROOMBA_GRACE_TIME))
                pids.append(p)
                p.start()
                p = Process(
                    target=self.vacuum,
                    args=(i,
                        settings.FULL_NAMESPACE,
                        settings.FULL_DURATION + settings.ROOMBA_GRACE_TIME))
                pids.append(p)
                p.start()

            # Send wait signal to zombie processes
            for p in pids:
                p.join()
