# Copyright (C) 2019 SignalFx, Inc. All rights reserved.
import mock

from celery_opentracing import CeleryTracing


class TestSignalHandlers(object):

    def test_registered_connect_with_propagate(self):
        with mock.patch('celery.utils.dispatch.signal.Signal.connect') as connect:
            app = CeleryTracing()
            assert connect.called
            call_args = [call.args[0] for call in connect.call_args_list]
            assert app._prepublish in call_args
            assert app._start_span in call_args
            assert app._tag_error in call_args
            assert app._tag_retry in call_args
            assert app._finish_span in call_args

    def test_registered_disconnect_with_propagate(self):
        with mock.patch('celery.utils.dispatch.signal.Signal.disconnect') as disconnect:
            app = CeleryTracing()
            app.disconnect_traced_handlers()
            assert disconnect.called
            call_args = [call.args[0] for call in disconnect.call_args_list]
            assert app._prepublish in call_args
            assert app._start_span in call_args
            assert app._tag_error in call_args
            assert app._tag_retry in call_args
            assert app._finish_span in call_args

    def test_registered_connect_without_propagate(self):
        with mock.patch('celery.utils.dispatch.signal.Signal.connect') as connect:
            app = CeleryTracing(propagate=False)
            assert connect.called
            call_args = [call.args[0] for call in connect.call_args_list]
            assert app._prepublish not in call_args
            assert app._start_span in call_args
            assert app._tag_error in call_args
            assert app._tag_retry in call_args
            assert app._finish_span in call_args

    def test_registered_disconnect_without_propagate(self):
        with mock.patch('celery.utils.dispatch.signal.Signal.disconnect') as disconnect:
            app = CeleryTracing(propagate=False)
            app.disconnect_traced_handlers()
            assert disconnect.called
            call_args = [call.args[0] for call in disconnect.call_args_list]
            assert app._prepublish not in call_args
            assert app._start_span in call_args
            assert app._tag_error in call_args
            assert app._tag_retry in call_args
            assert app._finish_span in call_args
