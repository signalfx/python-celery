# Copyright (C) 2019 SignalFx, Inc. All rights reserved.
from collections import namedtuple
import logging

from opentracing.mocktracer import MockTracer
from celery import Celery
import pytest

from celery_opentracing import CeleryTracing

logging.basicConfig(level='DEBUG')

broker = 'amqp://guest@localhost/'
backend = 'rpc://guest@localhost:5672/'


class TestUntracedCelery(object):
    # pytest uses several test class instances, so keep state here
    _tasks = dict(add=None, blowup=None)
    _tracer = dict(tracer=None)

    @pytest.fixture(scope='class')
    def celery_application(self, celery_active_broker, celery_result_backend):
        tracer = MockTracer()
        self._tracer['tracer'] = tracer

        with CeleryTracing(tracer=tracer, broker=celery_active_broker, backend=celery_result_backend) as traced:
            traced.finalize()
            with Celery(broker=celery_active_broker, backend=celery_result_backend) as app:

                @app.task
                def add(one, two):
                    assert self._tracer['tracer'].active_span is None
                    return one + two

                self._tasks['add'] = add

                @app.task
                def blowup():
                    return 1 / 0

                self._tasks['blowup'] = blowup

                yield app

    @pytest.fixture
    def tasks(self, celery_local_worker):
        yield namedtuple('tasks', ' '.join(self._tasks))(**dict(self._tasks.items()))

    @pytest.fixture
    def tracer(self):
        tracer = self._tracer['tracer']
        tracer.reset()
        yield tracer
        tracer.reset()

    def test_confirm_traced_application_doesnt_influence_untraced(self, tasks, tracer):
        add = tasks.add.delay(1, 2)
        assert add.get() == 3

        blowup = tasks.blowup.delay()
        with pytest.raises(Exception) as e:
            blowup.get()
        assert e.type.__name__ == 'ZeroDivisionError'  # account for celery 4.0 reraise type

        assert not tracer.finished_spans()
