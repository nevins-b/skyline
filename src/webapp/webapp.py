import redis
import logging
import simplejson as json
import sys
from msgpack import Unpacker
from flask import Flask, request, render_template
from daemon import runner
from os.path import dirname, abspath
from ring import RedisRing

# add the shared settings file to namespace
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import settings

RING = RedisRing(settings.REDIS_BACKENDS)

app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True

@app.route("/")
def index():
    return render_template('index.html'), 200

@app.route("/app_settings")
def app_settings():
    app_settings = {'GRAPHITE_HOST': settings.GRAPHITE_HOST,
                    'OCULUS_HOST': settings.OCULUS_HOST,
                    'MINI_NAMESPACE': settings.MINI_NAMESPACE,
                    'FULL_NAMESPACE': settings.FULL_NAMESPACE
                   }
    resp = json.dumps(app_settings)
    return resp, 200

@app.route("/api", methods=['GET'])
def data():
    metric = request.args.get('metric', None)
    try:
        raw_series = REDIS_CONN.get(metric)
        if not raw_series:
            resp = json.dumps({'results': 'Error: No metric by that name'})
            return resp, 404
        else:
            unpacker = Unpacker(use_list = False)
            unpacker.feed(raw_series)
            timeseries = [item[:2] for item in unpacker]
            resp = json.dumps({'results': timeseries})
            return resp, 200
    except Exception as e:
        error = "Error: " + e
        resp = json.dumps({'results': error})
        return resp, 500

@app.route("/dump/anomalies.json", methods=['GET'])
def anomalies():
    resp = 'handle_data([])'
    try:
        analyzer_key_node = REDIS_BACKENDS.get_node(settings.ANALYZER_ANOMALY_KEY)
        anomaly_keys = RING.run('smembers', settings.ANALYZER_ANOMALY_KEY)
        anomalies = {}
        if not anomaly_keys:
            logger.info("No anomaly key found!")
            return resp, 200
        for key in list(anomaly_keys):
            raw_anomalies = RING.run('get',key)
            if not raw_anomalies:
                logger.info("Can't get anomalies for key %s, removing it from set" % key)
                RING.run('srem', settings.ANALYZER_ANOMALY_KEY, key)
                continue
            unpacker = Unpacker(use_list = False)
            unpacker.feed(raw_anomalies)
            for item in unpacker:
                anomalies.update(item)
        anomaly_list = []
        for anom, value in anomalies.iteritems():
                anomaly_list.append([value, anom])
        if len(anomaly_list) > 0:
            anomaly_list.sort(key=operator.itemgetter(1))
            resp = 'handle_data(%s)' % anomaly_list
    except Exception as e:
        logger.error("Error getting anomalies: %s" % str(e))
    return resp, 200

class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = settings.LOG_PATH + '/webapp.log'
        self.stderr_path = settings.LOG_PATH + '/webapp.log'
        self.pidfile_path =  settings.PID_PATH + '/webapp.pid'
        self.pidfile_timeout = 5

    def run(self):

        logger.info('starting webapp')
        logger.info('hosted at %s' % settings.WEBAPP_IP)
        logger.info('running on port %d' % settings.WEBAPP_PORT)

        app.run(settings.WEBAPP_IP, settings.WEBAPP_PORT)

if __name__ == "__main__":
    """
    Start the server
    """

    webapp = App()

    logger = logging.getLogger("AppLog")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler(settings.LOG_PATH + '/webapp.log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        webapp.run()
    else:
        daemon_runner = runner.DaemonRunner(webapp)
        daemon_runner.daemon_context.files_preserve=[handler.stream]
        daemon_runner.do_action()
