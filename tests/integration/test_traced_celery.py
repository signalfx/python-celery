# Copyright (C) 2019 SignalFx, Inc. All rights reserved.
from collections import namedtuple
import logging

from celery import signals, Celery, __version__
from opentracing.mocktracer import MockTracer
import opentracing.ext.tags as ext_tags
from opentracing import Tracer
import celery.exceptions
import pytest

from celery_opentracing import CeleryTracing

logging.basicConfig(level='DEBUG')

version_31 = __version__[0:3] == '3.1'


class TestTracedCelery(object):
    # pytest uses several test class instances, so keep state here
    _tasks = {}
    _tracer = {}

    @pytest.fixture(scope='class')
    def celery_application(self, celery_active_broker, celery_result_backend):
        tracer = MockTracer()
        self._tracer['tracer'] = tracer

        with CeleryTracing(tracer=tracer, broker=celery_active_broker, backend=celery_result_backend) as app:

            @app.task
            def add(one, two):
                span = tracer.active_span
                span.set_tag('one', one)
                span.set_tag('two', two)
                return one + two

            self._tasks['add'] = add

            @app.task
            def blowup():
                return 1 / 0

            self._tasks['blowup'] = blowup

            @app.task(bind=True)
            def retry(self, raze=True):
                if not raze:
                    return 'Success'

                try:
                    raise Exception('My Exception!')
                except Exception as exc:
                    self.retry(args=(False,), exc=exc, countdown=.5)

            self._tasks['retry'] = retry

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

    def test_app_identity(self, celery_app):
        assert isinstance(celery_app, Celery)
        assert not isinstance(celery_app, Tracer)

    def test_registered_signals(self, celery_app):
        celery_app.disconnect_traced_handlers()

        for signal in (signals.before_task_publish, signals.task_prerun, signals.task_failure, signals.task_retry,
                       signals.task_postrun):
            assert not signal.receivers

        celery_app.connect_traced_handlers()

        for signal, handler in [(signals.before_task_publish, celery_app._prepublish),
                                (signals.task_prerun, celery_app._start_span),
                                (signals.task_failure, celery_app._tag_error),
                                (signals.task_retry, celery_app._tag_retry),
                                (signals.task_postrun, celery_app._finish_span)]:
            assert signal.receivers[0][1] == handler

    def test_basic_propagation(self, tasks, tracer):
        r = tasks.add.delay(1, 2)
        assert r.get() == 3
        spans = tracer.finished_spans()
        assert len(spans) == 2

        parent, child = spans
        assert parent.context.trace_id == child.context.trace_id
        assert parent.operation_name == 'publish test_traced_celery.add'
        assert parent.parent_id is None
        assert parent.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert parent.tags.pop('celery.delivery.exchange') == 'celery'
        assert parent.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_PRODUCER,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery'
        }

        assert child.operation_name == 'test_traced_celery.add'
        assert child.parent_id == parent.context.span_id
        assert child.tags.pop('celery.worker.hostname', False)
        assert child.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert child.tags.pop('celery.delivery.exchange') == 'celery'
        assert child.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_CONSUMER,
            'celery.delivery.priority': 0,
            'celery.delivery.redelivered': False,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery',
            'one': 1,
            'two': 2
        }

    def test_basic_propagation_with_countdown(self, tasks, tracer):
        r = tasks.add.apply_async((2, 3), countdown=.5)
        assert r.get() == 5
        spans = tracer.finished_spans()
        assert len(spans) == 2

        parent, child = spans
        assert parent.context.trace_id == child.context.trace_id
        assert parent.operation_name == 'publish test_traced_celery.add'
        assert parent.parent_id is None
        assert parent.tags.pop('celery.eta', version_31)
        assert parent.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert parent.tags.pop('celery.delivery.exchange') == 'celery'
        assert parent.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_PRODUCER,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery'
        }

        assert child.operation_name == 'test_traced_celery.add'
        assert child.parent_id == parent.context.span_id
        assert child.tags.pop('celery.eta', False)
        assert child.tags.pop('celery.worker.hostname', False)
        assert child.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert child.tags.pop('celery.delivery.exchange') == 'celery'
        assert child.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_CONSUMER,
            'celery.delivery.priority': 0,
            'celery.delivery.redelivered': False,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery',
            'one': 2,
            'two': 3
        }

    def test_propagation_with_exception_in_task(self, tasks, tracer):
        r = tasks.blowup.delay()
        with pytest.raises(Exception) as e:
            r.get()
        assert e.type.__name__ == 'ZeroDivisionError'  # account for celery 4.0 reraise type

        spans = tracer.finished_spans()
        assert len(spans) == 2

        parent, child = spans
        assert parent.context.trace_id == child.context.trace_id
        assert parent.operation_name == 'publish test_traced_celery.blowup'
        assert parent.parent_id is None
        assert parent.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert parent.tags.pop('celery.delivery.exchange') == 'celery'
        assert parent.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_PRODUCER,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery'
        }

        assert child.operation_name == 'test_traced_celery.blowup'
        assert child.parent_id == parent.context.span_id
        assert child.tags.pop('celery.worker.hostname', False)
        assert child.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert child.tags.pop('celery.delivery.exchange') == 'celery'
        tags = {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_CONSUMER,
            'celery.delivery.priority': 0,
            'celery.delivery.redelivered': False,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'error': True,
            'message_bus.destination': 'celery',
        }

        for k, v in tags.items():
            assert k in child.tags
            assert child.tags[k] == v

        assert child.tags['sfx.error.kind'] == 'ZeroDivisionError'
        assert isinstance(child.tags['sfx.error.object'], ZeroDivisionError)
        assert 'by zero' in child.tags['sfx.error.message']
        assert 'sfx.error.stack' in child.tags

    def test_propagation_with_retry_and_exception_in_task(self, tasks, tracer):
        r = tasks.retry.delay()

        result = None
        for i in range(5):
            try:
                # For some reason result unavailable after initial get
                result = r.get(timeout=1)
            except celery.exceptions.TimeoutError:
                if i == 4:
                    raise

        assert result == 'Success'
        spans = tracer.finished_spans()
        assert len(spans) == 4

        parent, retry_parent, first_run, final_run = spans
        assert parent.context.trace_id == first_run.context.trace_id
        assert parent.operation_name == 'publish test_traced_celery.retry'
        assert parent.parent_id is None
        assert parent.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert parent.tags.pop('celery.delivery.exchange') == 'celery'
        assert parent.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_PRODUCER,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery'
        }

        assert first_run.operation_name == 'test_traced_celery.retry'
        assert first_run.parent_id == parent.context.span_id
        assert first_run.tags.pop('celery.retry.reason', False)
        assert first_run.tags.pop('celery.worker.hostname', False)
        assert first_run.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert first_run.tags.pop('celery.delivery.exchange') == 'celery'
        tags = {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_CONSUMER,
            'celery.delivery.priority': 0,
            'celery.delivery.redelivered': False,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 0,
            'celery.retry': True,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery',
            'error': True,
            'sfx.error.kind': 'Retry',
        }
        for k, v in tags.items():
            assert k in first_run.tags
            assert first_run.tags[k] == v

        assert isinstance(first_run.tags['sfx.error.object'], celery.exceptions.Retry)
        assert "Retry in 0.5s: Exception('My Exception!'" in first_run.tags['sfx.error.message']
        assert 'sfx.error.stack' in first_run.tags

        assert retry_parent.context.trace_id == first_run.context.trace_id
        assert retry_parent.operation_name == 'publish test_traced_celery.retry'
        assert retry_parent.parent_id == first_run.context.span_id
        assert retry_parent.tags.pop('celery.eta', False)
        assert retry_parent.tags.pop('celery.worker.hostname', False)
        assert retry_parent.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert retry_parent.tags.pop('celery.delivery.exchange') == 'celery'
        assert retry_parent.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_PRODUCER,
            'celery.delivery.priority': 0,
            'celery.delivery.redelivered': False,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 1,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery'
        }

        assert final_run.operation_name == 'test_traced_celery.retry'
        assert final_run.parent_id == retry_parent.context.span_id
        assert final_run.tags.pop('celery.eta', False)
        assert final_run.tags.pop('celery.worker.hostname', False)
        assert final_run.tags.pop('celery.task.origin', version_31)
        if version_31:
            assert final_run.tags.pop('celery.delivery.priority') == 0
            assert final_run.tags.pop('celery.delivery.exchange') == 'celery'
        assert final_run.tags == {
            ext_tags.COMPONENT: 'celery',
            ext_tags.SPAN_KIND: ext_tags.SPAN_KIND_CONSUMER,
            'celery.delivery.redelivered': False,
            'celery.delivery.routing_key': 'celery',
            'celery.retries': 1,
            'celery.task.id': r.id,
            'message_bus.destination': 'celery'
        }
