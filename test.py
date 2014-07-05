#!/usr/bin/env python

import cProfile
from contextlib import contextmanager
import time
from datetime import datetime
import logging
import sys
import threading

import eventlet
from oslo.config import cfg
from oslo import messaging


LOG = logging.getLogger(__name__)

TOTAL_BANDWIDTH = 0


class TestClient(threading.Thread):

    def __init__(self, num_calls, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)
        self._num_calls = num_calls
        self.transport = messaging.get_transport(cfg.CONF, 'rabbit://guest:guest@localhost:5672/')
        target = messaging.Target(topic='testtopic', version='1.0')
        self._client = messaging.RPCClient(self.transport, target)

    def run(self):
        global TOTAL_BANDWIDTH
        for i in range(self._num_calls):
            self._client.cast({}, 'test', arg=self.name)
            TOTAL_BANDWIDTH += 1


class TestEndpoint(object):

    def __init__(self):
        self.buffer = []
        self.buffer_size = 1000

    def test(self, ctx, arg):
        global TOTAL_BANDWIDTH
        TOTAL_BANDWIDTH += 1
        self.buffer.append(arg)
        if len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        LOG.info('Received %s messages' % len(self.buffer))
        self.buffer = []


def client():
    eventlet.monkey_patch()
    CLIENTS = 5
    MESSAGES_PER_CLIENT = 1000000
    workers = []
    for i in range(CLIENTS):
        t = TestClient(MESSAGES_PER_CLIENT)
        t.start()
        workers.append(t)
    for w in workers:
        w.join()

def server():
    eventlet.monkey_patch()

    transport = messaging.get_transport(cfg.CONF, 'rabbit://guest:guest@localhost:5672/')
    target = messaging.Target(topic='testtopic', server='server1', version='1.0')
    server = messaging.get_rpc_server(transport, target, [TestEndpoint()],
                                      executor='eventlet')
    server.start()
    server.wait()


@contextmanager
def profiler(name):
    profiler = cProfile.Profile()
    profiler.enable()

    yield

    profiler.disable()
    timestamp = datetime.now().strftime('%d%m_%H:%M:%S')
    profiler.dump_stats('%s-%s.pstats' % name, timestamp)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,level=logging.WARNING)
    argv = sys.argv[1:]
    start_server = '-s' in argv
    profiler_enabled = '-p' in argv

    #cfg.CONF(argv, project='test')
    service = server if start_server else client

    start = time.time()
    try:
        if profiler_enabled:
            with profiler(service.__name__):
                service()
        else:
            service()
    except KeyboardInterrupt:
        pass  # gracefully exit
    run_time = time.time() - start
    bandwidth = TOTAL_BANDWIDTH / run_time
    LOG.warn('Run time: %s' % run_time)
    LOG.warn('Total bandwidth: %s m/s' % bandwidth)
