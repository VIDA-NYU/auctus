import http.server
import json
import socketserver
import threading


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    _instance = None

    @staticmethod
    def instance():
        if not Server._instance:
            Server._instance = Server()
        return Server._instance

    def __init__(self):
        super(Server, self).__init__(('', 8001), ServerHandler)
        self.services = {}
        self._thread = threading.Thread(target=self.serve_forever)
        self._thread.daemon = True
        self._thread.start()


class ServerHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        name, method = self.path[1:].split('/', 1)
        method = 'do_' + method
        for handler in self.server.services.get(name):
            if hasattr(handler, method):
                out = getattr(handler, method)()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(json.dumps(out))
