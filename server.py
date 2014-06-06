#!/usr/bin/env python

import cProfile
from datetime import datetime
import logging
import os
import sys

import eventlet

from oslo.config import cfg
from oslo import messaging
from oslo.messaging import opts

logging.basicConfig(stream=sys.stdout,level=logging.WARNING)

LOG = logging.getLogger(__name__)


class TestEndpoint(object):

    def __init__(self):
        self.buffer = []
        self.buffer_size = 1000

    def test(self, ctx, arg):
        self.buffer.append(arg)
        if len(self.buffer) > self.buffer_size:
            #import pdb; pdb.set_trace()
            self.flush()

    def flush(self):
        LOG.warn('Received %s messages' % len(self.buffer))
        self.buffer = []


eventlet.monkey_patch()

transport = messaging.get_transport(cfg.CONF, 'rabbit://guest:guest@localhost:5672/')
target = messaging.Target(topic='testtopic', server='server1', version='1.0')
server = messaging.get_rpc_server(transport, target, [TestEndpoint()],
                                  executor='eventlet')

def start_server():

    profiler = cProfile.Profile()
    try:
        profiler.enable()
        server.start()
        server.wait()
    except KeyboardInterrupt:
        profiler.disable()
        dir_name = os.path.dirname(os.path.abspath(__file__))
        file_name = 'server-%s.pstats' % datetime.now().strftime('%d%m_%H:%M:%S')
        profiler.dump_stats(os.path.join(dir_name, file_name))


if __name__ == '__main__':
    start_server()
