# Celery OpenTracing

This package enables tracing task execution in a [Celery 3.1+](http://www.celeryproject.org/) application via [The OpenTracing Project](http://opentracing.io). Once a production system contends with real concurrency or splits into many services, crucial (and formerly easy) tasks become difficult: user-facing latency optimization, root-cause analysis of backend errors, communication about distinct pieces of a now-distributed system, etc. Distributed tracing follows a request on its journey from inception to completion from mobile/browser all the way to the microservices.

As core services and libraries adopt OpenTracing, the application builder is no longer burdened with the task of adding basic tracing instrumentation to their own code. In this way, developers can build their applications with the tools they prefer and benefit from built-in tracing instrumentation. OpenTracing implementations exist for major distributed tracing systems and can be bound or swapped with a one-line configuration change.

If you want to learn more about the underlying Python API, visit the Python [source code](https://github.com/signalfx/python-celery/).

## Installation

Run the following command:

```
$ pip install signalfx-instrumentationn-celery
```

## Usage

The provided `celery.app.base.Celery` subclass allows the tracing of task scheduling and execution using the OpenTracing API. All that it requires is for a `CeleryTracing` instance to be initialized using an instance of an OpenTracing tracer and treated as a standard `Celery` application.

### Initialize

`CeleryTracing` takes the `Tracer` instance that is supported by OpenTracing and an optional dictionary of desired tags for each created span. You can also specify whether you'd like the initial task publish events to be represented by a span whose trace context is propagated via request headers to the worker executing the task (enabled by default).  To create a `CeleryTracing` object, you can either pass in a tracer object directly or default to the `opentracing.tracer` global tracer that's set elsewhere in your application:

```python
from celery_opentracing import CeleryTracing

opentracing_tracer = # some OpenTracing tracer implementation
traced_app = CeleryTracing(tracer=opentracing_tracer, propagate=True,  # propagation allows distributed tracing from
                           span_tags=dict(my_helpful='tag'))           # publisher over broker to workers.

@traced_app.task(bind=True)
def my_task(self):
    return True
```

or

```python
from celery_opentracing import CeleryTracing
import opentracing

opentracing.tracer = # some OpenTracing tracer implementation
traced_app = CeleryTracing(propagate=False)  # Tracer defaults to opentracing.tracer.  No publish span creation or propagation to worker execution context will configured as well.

@traced_app.task(bind=True)
def my_task(self):
    return True
```

## Further Information

If you're interested in learning more about the OpenTracing standard, please visit [opentracing.io](http://opentracing.io/).  If you would like to implement OpenTracing in your project and need help, feel free to send us a note at [community@opentracing.io](community@opentracing.io).
