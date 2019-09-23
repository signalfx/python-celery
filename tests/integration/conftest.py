# Copyright (C) 2019 SignalFx, Inc. All rights reserved.
import threading
import time
import os

from celery.result import _set_task_join_will_block
from celery import Celery, shared_task, worker
from kombu import Connection
import docker
import pytest


@pytest.fixture(scope='session')
def rabbitmq_container():
    client = docker.from_env()
    cwd = os.path.dirname(os.path.abspath(__file__))
    conf = os.path.join(cwd, 'conf')
    volumes = ['{}/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf'.format(conf)]
    rabbit = client.containers.run(
        'rabbitmq:latest',
        ports={'4639/tcp': 4639,
               '5462/tcp': 5462,
               '5671/tcp': 5671,
               '5672/tcp': 5672,
               '25672/tcp': 25672},
        volumes=volumes,
        detach=True)
    try:
        yield rabbit
    finally:
        rabbit.remove(v=True, force=True)


@shared_task(name='pytest.conftest.celery.is_ready')
def is_ready(*args, **kwargs):
    return True


class Worker(worker.WorkController):

    def __init__(self, *args, **kwargs):
        self._has_started = threading.Event()
        worker.WorkController.__init__(self, *args, **kwargs)

    def on_consumer_ready(self, consumer):
        self._has_started.set()

    def wait(self):
        self._has_started.wait()

    def start(self, *args, **kwargs):
        return super(Worker, self).start(*args, **kwargs)


@pytest.fixture(scope='session')
def celery_broker():
    return 'amqp://guest@localhost/'


@pytest.fixture(scope='session')
def celery_active_broker(celery_broker, rabbitmq_container):
    connection = Connection(celery_broker)
    for i in range(1, 21):
        try:
            channel = connection.channel()
            channel.close()
            return celery_broker
        except IOError:
            print('Waiting for connection...')
            if i == 20:
                raise Exception('Cannot reach broker.')
            time.sleep(1)


@pytest.fixture(scope='session')
def celery_result_backend():
    return 'rpc://guest@localhost:5672/'


@pytest.fixture(scope='class')
def celery_application(celery_active_broker, celery_result_backend):
    with Celery(broker=celery_active_broker, backend=celery_result_backend) as app:
        yield app


@pytest.fixture(scope='class')
def celery_app(celery_application):
    celery_application.conf.broker_heartbeat = 0
    celery_application.finalize()
    celery_application.set_current()
    celery_application.set_default()
    return celery_application


@pytest.fixture(scope='class')
def celery_local_worker(celery_active_broker, celery_app):
    """
    A threaded worker.  Because of event loop issues for different kombu versions
    (https://github.com/celery/celery/issues/4088), it's advised to have separate
    pytest invocations for each sourcing test class to better ensure setup/teardown.
    """
    worker = Worker(
        app=celery_app,
        concurrency=1,
        prefetch_multiplier=0,
        pool_cls='solo',
        optimization='fair',
        ready_callback=None,
        without_mingle=True,
        without_gossip=True,
        without_heartbeat=True
    )

    t = threading.Thread(target=worker.start)
    t.daemon = True
    t.start()
    worker.wait()

    _set_task_join_will_block(False)
    assert is_ready.delay().get(timeout=10) is True

    celery_app.control.purge()
    yield worker
    celery_app.control.purge()
    time.sleep(2)  # avoid race condition where purge() double acks termination messages

    from celery.worker import state
    state.should_terminate = True
    t.join(10)
    state.should_terminate = None
