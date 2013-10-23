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

from relay import Relay

# TODO: http://stackoverflow.com/questions/6728236/exception-thrown-in-multiprocessing-pool-not-detected

class RelayAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = settings.LOG_PATH + '/relay.log'
        self.stderr_path = settings.LOG_PATH + '/relay.log'
        self.pidfile_path = settings.PID_PATH + '/relay.pid'
        self.pidfile_timeout = 5

    def run(self):
        logger.info('starting relay agent')
        pid = getpid()
        Relay().start()
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

    relay = RelayAgent()

    logger = logging.getLogger("RelayLog")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler(settings.LOG_PATH + '/relay.log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        relay.run()
    else:
        daemon_runner = runner.DaemonRunner(relay)
        daemon_runner.daemon_context.files_preserve=[handler.stream]
        daemon_runner.do_action()
