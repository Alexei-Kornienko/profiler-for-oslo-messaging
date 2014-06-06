#!/usr/bin/env python

import cProfile
from datetime import datetime
import logging
import sys
import threading

import eventlet
from oslo.config import cfg
from oslo import messaging

from oslo.messaging._drivers import amqpdriver

CLIENTS = 100
CLIENT_MESSAGES = 10000
LOG = logging.getLogger(__name__)

class TestClient(threading.Thread):

    def __init__(self, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)
        self.transport = messaging.get_transport(cfg.CONF, 'rabbit://guest:guest@localhost:5672/')
        target = messaging.Target(topic='testtopic', version='1.0')
        self._client = messaging.RPCClient(self.transport, target)


    def run(self):
        for i in range(CLIENT_MESSAGES):
            self._client.cast({}, 'test', arg=self.name)


def send_messages(n):
    for i in range(n):
        t = TestClient()
        t.run()

def start_client():
    eventlet.monkey_patch()
    #profiler = cProfile.Profile()
    try:
        #profiler.enable()
        #argv = sys.argv
        #cfg.CONF(argv[1:], project='test')
        send_messages(CLIENTS)
    except KeyboardInterrupt:
        pass
    finally:
        pass
        #profiler.disable()
        #file_name = 'client-%s.pstats' % datetime.now().strftime('%d%m_%H:%M:%S')
        #profiler.dump_stats(file_name)


if __name__ == '__main__':
    start_client()
