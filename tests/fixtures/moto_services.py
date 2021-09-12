# Copyright 2019-2021 Darren Weber
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import logging
import os
import socket
import threading
import time

# Third Party
import moto.backends
import moto.server
import urllib3
import werkzeug.serving

HOST = "127.0.0.1"

_PYCHARM_HOSTED = os.environ.get("PYCHARM_HOSTED") == "1"
CONNECT_TIMEOUT = 90 if _PYCHARM_HOSTED else 10


def get_free_tcp_port(release_socket: bool = False):
    sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt.bind(("", 0))
    addr, port = sckt.getsockname()
    if release_socket:
        sckt.close()
        return port

    return sckt, port


class MotoService:
    """Will Create MotoService.
    Service is ref-counted so there will only be one per process. Real Service will
    be returned by `__aenter__`."""

    _services = dict()  # {name: instance}

    def __init__(self, service_name: str, port: int = None):
        self._service_name = service_name

        if port:
            self._socket = None
            self._port = port
        else:
            self._socket, self._port = get_free_tcp_port()

        self._thread = None
        self._logger = logging.getLogger("MotoService")
        self._refcount = 0
        self._ip_address = HOST
        self._server = None

    @property
    def endpoint_url(self):
        return "http://{}:{}".format(self._ip_address, self._port)

    def reset(self):
        # each service can have multiple regional backends
        service_backends = moto.server.backends.get_backend(self._service_name)
        for region_name, backend in service_backends.items():
            backend.reset()

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            self._start()
            try:
                result = func(*args, **kwargs)
            finally:
                self._stop()
            return result

        functools.update_wrapper(wrapper, func)
        wrapper.__wrapped__ = func
        return wrapper

    def __enter__(self):
        svc = self._services.get(self._service_name)
        if svc is None:
            self._services[self._service_name] = self
            self._refcount = 1
            self._start()
            return self
        else:
            svc._refcount += 1
            return svc

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._refcount -= 1

        if self._socket:
            self._socket.close()
            self._socket = None

        if self._refcount == 0:
            del self._services[self._service_name]
            self._stop()

    def _server_entry(self):
        self._main_app = moto.server.DomainDispatcherApplication(
            moto.server.create_backend_app, service=self._service_name
        )
        self._main_app.debug = True

        if self._socket:
            self._socket.close()  # release right before we use it
            self._socket = None

        self._server = werkzeug.serving.make_server(
            self._ip_address, self._port, self._main_app, True
        )
        self._server.serve_forever()

    def _start(self):
        self._thread = threading.Thread(target=self._server_entry, daemon=True)
        self._thread.start()

        http = urllib3.PoolManager()

        start = time.time()

        while time.time() - start < 10:
            if not self._thread.is_alive():
                break

            try:
                resp = http.request(
                    "GET", self.endpoint_url + "/static", timeout=CONNECT_TIMEOUT
                )
                break
            except (
                urllib3.exceptions.NewConnectionError,
                urllib3.exceptions.MaxRetryError,
            ):
                time.sleep(0.5)
        else:
            self._stop()  # pytest.fail doesn't call stop_process
            raise Exception("Cannot start MotoService: {}".format(self._service_name))

    def _stop(self):
        if self._server:
            self._server.shutdown()

        self._thread.join()
