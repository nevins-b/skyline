import logging
import time
import sys
from os import getpid
from os.path import dirname, abspath, isdir
from daemon import runner
import zmq

# add the shared settings file to namespace
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import settings

from listen import Listen
from roomba import Roomba
from worker import Worker

# TODO: http://stackoverflow.com/questions/6728236/exception-thrown-in-multiprocessing-pool-not-detected


class Horizon():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = settings.LOG_PATH + '/horizon.log'
        self.stderr_path = settings.LOG_PATH + '/horizon.log'
        self.pidfile_path = settings.PID_PATH + '/horizon.pid'
        self.pidfile_timeout = 5

    def run(self):
        logger.info('starting horizon agent')
        pid = getpid()
        context = zmq.Context()
        # Start the workers
        for i in range(settings.WORKER_PROCESSES):
            if i == 0:
                Worker(pid, context, canary=True).start()
            else:
                Worker(pid, context).start()

        # Start the roomba
        Roomba(pid).start()

        # Keep yourself occupied, sucka
        while 1:
            time.sleep(100)

if __name__ == "__main__":
    """
    Start the Horizon agent.
    """
    if not isdir(settings.PID_PATH):
        print 'pid directory does not exist at %s' % settings.PID_PATH
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print 'log directory does not exist at %s' % settings.LOG_PATH
        sys.exit(1)

    horizon = Horizon()

    logger = logging.getLogger("HorizonLog")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler(settings.LOG_PATH + '/horizon.log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        horizon.run()
    else:
        daemon_runner = runner.DaemonRunner(horizon)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()
