import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread

logger = logging.getLogger(__name__)
_health_lock = Lock()
_health_state = {
    "service": "starting",
    "bot": "starting",
    "db": "unknown",
    "last_error": "",
}


class HealthServer(ThreadingHTTPServer):
    allow_reuse_address = True


class HealthHandler(BaseHTTPRequestHandler):
    server_version = "MovieBotHealth/1.0"

    def do_GET(self):
        self._handle_request(send_body=True)

    def do_HEAD(self):
        self._handle_request(send_body=False)

    def _handle_request(self, *, send_body):
        if self.path == "/":
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
        elif self.path == "/healthz":
            with _health_lock:
                payload = dict(_health_state)
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
        else:
            body = b"Not Found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")

        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def log_message(self, format, *args):
        return


def set_health_state(*, service=None, bot=None, db=None, last_error=None):
    with _health_lock:
        if service is not None:
            _health_state["service"] = service
        if bot is not None:
            _health_state["bot"] = bot
        if db is not None:
            _health_state["db"] = db
        if last_error is not None:
            _health_state["last_error"] = last_error


def run():
    port = int(os.environ.get("PORT", "8080"))
    logger.info("Health server %s-portda ishga tushmoqda", port)
    server = HealthServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()


def keep_alive():
    if getattr(keep_alive, "_started", False):
        return
    t = Thread(target=run)
    t.daemon = True
    t.start()
    keep_alive._started = True
    set_health_state(service="running")
