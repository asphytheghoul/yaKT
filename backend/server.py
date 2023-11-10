from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from queue import Queue
from record_manager import RecordManager
from api_endpoints import handle_request
from database import Database
import signal
import sys
import json

# Create a handler for the SIGINT signal
def signal_handler(sig, frame):
    print('Shutting down server...')
    # Terminate the program
    sys.exit(0)

# Register the signal handler for the SIGINT signal
signal.signal(signal.SIGINT, signal_handler)

server_pool = Queue()
record_manager = RecordManager(Database())

class RequestHandler(BaseHTTPRequestHandler):
   def do_GET(self):
        thread = Thread(target=handle_request, args=(self.path, self, record_manager))
        thread.start()
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(bytes(json.dumps({"status": "ok"}), 'utf-8'))

def run(server_class=HTTPServer, handler_class=RequestHandler):
   server_address = ('', 8000)
   print('Starting server...')
   print('Listening on port 8000...')
   print('Dev server is up and ready to go!')
   for _ in range(10):
       server_pool.put(server_class(server_address, handler_class))
   while not server_pool.empty():
       server = server_pool.get()
       try:
           server.serve_forever()
       except KeyboardInterrupt:
           pass
       server.server_close()
   print('Server stopped.')

run()
