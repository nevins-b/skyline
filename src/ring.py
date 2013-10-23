
from hash_ring import HashRing
from redis import StrictRedis, WatchError

MAX_FAILURES = 3
class RedisRing:
	def __init__(self, backends):
		self.map = HashRing()
		self.state = {
			'live': {},
			'failed': {},
		}
        for backend in backends:
        	self._connect(backend)

    def _connect(self, connection_string):
    	self.live.append(connection_string)
        if connection_string.startswith('unix://'):
            socket = connection_string.replace('unix://','')
            conn = StrictRedis(unix_socket_path=socket)
        else:
            conn = StrictRedis.from_url(connection_string)
        self.state['live'][conn]= backend
        self.map.add_node(conn)

    def get_connection(self, string_key):
    	return self.map.get_node(string_key)

   	def check_connections(self):
   		for conn in self.map.nodes:
   			while True:
	   			try:
	   				conn.ping()
	   				break
	   			except:
	   				self.map.remove_node(conn)
	   				backend = self.state['live'].get(conn)
	   				del(self.state['live'][conn])
	   				failures = self.state['failed'].get(backend, 0)
	   				failures += 1
	   				self.state['failed'][backend] = failures
	   				if failures < MAX_FAILURES:
	   					self._connect(backend)
	   				else:
	   					break
	   	if len(self.state['live']) == 0:
	   		raise Exception('No live redis backends!')

	def run(self, func, key, *args):
		conn = self.hash_ring.get_node(key)
		try:
			if hashattr(conn, func):
				method = getattr(conn, func)
				if callable(method):
					if args:
						return method(key, *args)
					else:
						return method(key)
		except:
			raise

