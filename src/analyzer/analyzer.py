import logging
from time import time, sleep
from threading import Thread
from collections import defaultdict
from multiprocessing import Process, Manager, Lock
from msgpack import Unpacker, unpackb, packb
from os import path, kill, getpid, system
from math import ceil
import traceback
import operator
import settings
import socket
from ring import RedisRing

from alerters import trigger_alert
from algorithms import run_selected_algorithm
from algorithm_exceptions import *

logger = logging.getLogger("AnalyzerLog")

class Analyzer(Thread):
    def __init__(self, parent_pid):
        """
        Initialize the Analyzer
        """
        super(Analyzer, self).__init__()
        self.ring = RedisRing(settings.REDIS_BACKENDS)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.lock = Lock()
        self.exceptions = Manager().dict()
        self.anomaly_breakdown = Manager().dict()
        self.anomalous_metrics = Manager().list()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def spin_process(self, i, unique_metrics):
        process_key = '.'.join(['skyline','analyzer', socket.gethostname(), str(i)])
        alive_key = '.'.join([process_key, 'alive'])
        self.ring.run('set', alive_key, 1)
        self.ring.run('expire', alive_key, 30)
        """
        Assign a bunch of metrics for a process to analyze.
        """
        processes = list(self.ring.run('zrange', settings.ANALYZER_PROCESS_KEY, 0, -1))
        for key in processes:
            value = self.ring.run('get', key)
            if not value:
                self.ring.run('zrem', settings.ANALYZER_PROCESS_KEY, 0, key)

        # Add current process to index and determine position
        if not self.ring.run('zscore', settings.ANALYZER_PROCESS_KEY, alive_key):
            self.ring.run('zadd', settings.ANALYZER_PROCESS_KEY, time(), alive_key)
        self.ring.run('expire', settings.ANALYZER_PROCESS_KEY, 60)
        process_position = self.ring.run('zrank', settings.ANALYZER_PROCESS_KEY, alive_key) + 1
        process_count = self.ring.run('zcard', settings.ANALYZER_PROCESS_KEY)

        # If there are less processes then we know are going to be running assume
        # the others will start
        if process_count < settings.ANALYZER_PROCESSES:
            process_count = settings.ANALYZER_PROCESSES

        # Discover assigned metrics
        keys_per_processor = int(ceil(float(len(unique_metrics)) / float(process_count)))
        if process_position == process_count:
            assigned_max = len(unique_metrics)
        else:
            assigned_max = process_position * keys_per_processor
        assigned_min = assigned_max - keys_per_processor
        assigned_keys = range(assigned_min, assigned_max)

        # Compile assigned metrics
        assigned_metrics = [unique_metrics[index] for index in assigned_keys]

        # Check if this process is unnecessary
        if len(assigned_metrics) == 0:
            return

        # Multi get series
        raw_assigned = self.ring.run('mget', assigned_metrics)

        # Make process-specific dicts
        exceptions = defaultdict(int)
        anomaly_breakdown = defaultdict(int)

        # Distill timeseries strings into lists
        for i, metric_name in enumerate(assigned_metrics):
            self.check_if_parent_is_alive()

            try:
                raw_series = raw_assigned[i]
                unpacker = Unpacker(use_list = False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)

                anomalous, ensemble, datapoint = run_selected_algorithm(timeseries, metric_name)

                # If it's anomalous, add it to list
                if anomalous:
                    base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    metric = [datapoint, base_name]
                    self.anomalous_metrics.append(metric)

                    # Get the anomaly breakdown - who returned True?
                    for index, value in enumerate(ensemble):
                        if value:
                            algorithm = settings.ALGORITHMS[index]
                            anomaly_breakdown[algorithm] += 1

            # It could have been deleted by the Roomba
            except TypeError:
                exceptions['DeletedByRoomba'] += 1
            except TooShort:
                exceptions['TooShort'] += 1
            except Stale:
                exceptions['Stale'] += 1
            except Incomplete:
                exceptions['Incomplete'] += 1
            except Boring:
                exceptions['Boring'] += 1
            except:
                exceptions['Other'] += 1
                logger.info(traceback.format_exc())

        # if anomalies detected Pack and Write anomoly data to Redis
        if len(anomalous_metrics) > 0:
            packed = Packer().pack(anomalous_metrics)
            self.ring.run('set', process_key, packed)
            # expire the key in 30s so anomalys don't show up for too long
            self.ring.run('expire', process_key, 30)
            self.ring.run('sadd', settings.ANALYZER_ANOMALY_KEY, process_key)
            # expire the key in 60s so anomalys don't show up for too long
            self.ring.run('expire', settings.ANALYZER_ANOMALY_KEY, 60)

        # Collate process-specific dicts to main dicts
        with self.lock:
            for key, value in anomaly_breakdown.items():
                if key not in self.anomaly_breakdown:
                    self.anomaly_breakdown[key] = value
                else:
        	        self.anomaly_breakdown[key] += value

            for key, value in exceptions.items():
                if key not in self.exceptions:
                    self.exceptions[key] = value
                else:
        	        self.exceptions[key] += value



    def run(self):
        """
        Called when the process intializes.
        """
        while 1:
            now = time()
            # Make sure Redis is up
            try:
                self.ring.check_connections()
            except:
                sleep(10)
                self.ring = RedisRing(settings.REDIS_BACKENDS)
                continue

            # Discover unique metrics
            unique_metrics = list(self.ring.run('smembers', settings.FULL_NAMESPACE + 'unique_metrics'))

            if len(unique_metrics) == 0:
                logger.info('no metrics in redis. try adding some - see README')
                sleep(10)
                continue

            # Spawn processes
            pids = []
            for i in range(1, settings.ANALYZER_PROCESSES + 1):
                if i > len(unique_metrics):
                    logger.info('WARNING: skyline is set for more cores than needed.')
                    break

                p = Process(target=self.spin_process, args=(i, unique_metrics))
                pids.append(p)
                p.start()

            # Send wait signal to zombie processes
            for p in pids:
                p.join()

            # Send alerts
            if settings.ENABLE_ALERTS:
                for alert in settings.ALERTS:
                    for metric in self.anomalous_metrics:
                        if alert[0] in metric[1]:
                            cache_key = 'last_alert.%s.%s' % (alert[1], metric[1])
                            try:
                                last_alert = self.ring.run('get', cache_key)
                                if not last_alert:
                                    self.ring.run('setex', cache_key, alert[2], packb(metric[0]))
                                    trigger_alert(alert, metric)

                            except Exception as e:
                                logger.error("couldn't send alert: %s" % e)

            # Log progress
            logger.info('seconds to run    :: %.2f' % (time() - now))
            logger.info('total metrics     :: %d' % len(unique_metrics))
            logger.info('total analyzed    :: %d' % (len(unique_metrics) - sum(self.exceptions.values())))
            logger.info('total anomalies   :: %d' % len(self.anomalous_metrics))
            logger.info('exception stats   :: %s' % self.exceptions)
            logger.info('anomaly breakdown :: %s' % self.anomaly_breakdown)

            # Log to Graphite
            if settings.GRAPHITE_HOST != '':
                host = settings.GRAPHITE_HOST.replace('http://', '')
                system('echo skyline.analyzer.run_time %.2f %s | nc -w 3 %s 2003' % ((time() - now), now, host))
                system('echo skyline.analyzer.total_analyzed %d %s | nc -w 3 %s 2003' % ((len(unique_metrics) - sum(self.exceptions.values())), now, host))

            # Check canary metric
            raw_series = self.ring.run('get', settings.FULL_NAMESPACE + settings.CANARY_METRIC)
            if raw_series is not None:
                unpacker = Unpacker(use_list = False)
                unpacker.feed(raw_series)
                timeseries = list(unpacker)
                time_human = (timeseries[-1][0] - timeseries[0][0]) / 3600
                projected = 24 * (time() - now) / time_human

                logger.info('canary duration   :: %.2f' % time_human)
                if settings.GRAPHITE_HOST != '':
                    host = settings.GRAPHITE_HOST.replace('http://', '')
                    system('echo skyline.analyzer.duration %.2f %s | nc -w 3 %s 2003' % (time_human, now, host))
                    system('echo skyline.analyzer.projected %.2f %s | nc -w 3 %s 2003' % (projected, now, host))


            # Reset counters
            self.anomalous_metrics[:] = []
            self.exceptions = Manager().dict()
            self.anomaly_breakdown = Manager().dict()

            # Sleep if it went too fast
            if time() - now < 5:
                logger.info('sleeping due to low run time...')
                sleep(10)

