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
from oslo.messaging.notify import notifier


LOG = logging.getLogger(__name__)

TOTAL_BANDWIDTH = 0
TRANSPORT = None 

CLIENTS = 1
MESSAGES_PER_CLIENT = 1000000

class TestClient(threading.Thread):

    def __init__(self, num_calls, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)
        self._num_calls = num_calls
        target = messaging.Target(topic='testtopic', version='1.0')
        self._client = messaging.RPCClient(TRANSPORT, target)

    def run(self):
        global TOTAL_BANDWIDTH
        for i in xrange(self._num_calls):
            #self._client.cast({'context':'foo'}, 'test', arg=self.name)
            result = self._client.call({}, 'test', arg=self.name)
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
        return 'BAM'

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        global TOTAL_BANDWIDTH
        TOTAL_BANDWIDTH += 1
        self.buffer.append(publisher_id)
        if len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        LOG.info('Received %s messages' % len(self.buffer))
        self.buffer = []


def client():
    eventlet.monkey_patch()
    workers = []
    for i in range(CLIENTS):
        t = TestClient(MESSAGES_PER_CLIENT)
        t.start()
        workers.append(t)
    for w in workers:
        w.join()

def server():
    eventlet.monkey_patch()

    target = messaging.Target(topic='testtopic', server='server1', version='1.0')
    server = messaging.get_rpc_server(TRANSPORT, target, [TestEndpoint()],
                                      executor='blocking')
    server.start()
    server.wait()


class NotifyClient(threading.Thread):

    def __init__(self, num_calls, *args, **kwargs):
        super(NotifyClient, self).__init__(*args, **kwargs)
        self._num_calls = num_calls
        self.notifier = notifier.Notifier(TRANSPORT, 'test.client', driver='messaging', topic='notifications')

    def run(self):
        global TOTAL_BANDWIDTH
        for i in xrange(self._num_calls):
            self.notifier.error({'context':'foobar'}, 'test-notification', {'some': 'useful payload'})
            TOTAL_BANDWIDTH += 1

def notifier_client():
    eventlet.monkey_patch()
    workers = []
    for i in range(CLIENTS):
        t = NotifyClient(MESSAGES_PER_CLIENT)
        t.start()
        workers.append(t)
    for w in workers:
        w.join()


def notify_listener():
    eventlet.monkey_patch()

    target = messaging.Target(topic='notifications', server='server1', version='1.0')
    server = messaging.get_notification_listener(TRANSPORT, [target], [TestEndpoint()], executor='eventlet')
    #server = messaging.get_notification_listener(TRANSPORT, [target], [TestEndpoint()], executor='blocking')
    server.start()
    server.wait()

@contextmanager
def profiler(name):
    profiler = cProfile.Profile()
    profiler.enable()

    yield

    profiler.disable()
    timestamp = datetime.now().strftime('%d%m_%H:%M:%S')
    profiler.dump_stats('%s-%s.pstats' % (name, timestamp))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,level=logging.WARNING)
    
    TRANSPORT = messaging.get_transport(cfg.CONF, 'zmq://localhost/')
    #TRANSPORT = messaging.get_transport(cfg.CONF, 'rabbit://localhost/')

    argv = sys.argv[1:]
    start_server = '-s' in argv
    profiler_enabled = '-p' in argv
    notifier_test = '-n' in argv

    if notifier_test:
        service = notify_listener if start_server else notifier_client
    else:
        service = server if start_server else client

    start = time.time()
    if profiler_enabled:
        with profiler(service.__name__):
            try:
                service()
            except KeyboardInterrupt:
                pass  # gracefully exit
    else:
        try:
            service()
        except KeyboardInterrupt:
            pass  # gracefully exit
    run_time = time.time() - start
    bandwidth = TOTAL_BANDWIDTH / run_time
    LOG.warn('Run time: %s' % run_time)
    LOG.warn('Total bandwidth: %s m/s' % bandwidth)
