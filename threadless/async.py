#
# Copyright (c) 2016 Gilles Chehade <gilles@poolp.org>
# Copyright (c) 2013 Eric Faurot <eric@faurot.net>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

import asyncio
import collections
import json
import random
import selectors
import time
import types
import urllib.parse

selector = selectors.SelectSelector()
loop = asyncio.SelectorEventLoop(selector)
asyncio.set_event_loop(loop)
loop = asyncio.get_event_loop()

import threadless.log


class Eventlet(object):

    timestamp = None
    
    def __init__(self, threadlet, name):
        self.threadlet = threadlet
        self.name = name

    def __repr__(self):
        return 'eventlet(%s)' % self.name
        
    def set(self, delay, earlier = True, later = True):
        self.set_at(time.time() + delay, earlier = earlier, later = later)

    def set_at(self, timestamp, earlier = True, later = True):
        if self.timestamp:
            if not earlier and self.timestamp > timestamp:
                return
            if not later and self.timestamp < timestamp:
                return
        self.threadlet.debug('threadlet: %s: eventlet(%s): set: dt=%.02f',
                             self.threadlet.name,
                             self.name,
                             timestamp - time.time())
        self.timestamp = timestamp
        self.threadlet.timeouts.add(self)
        self.threadlet.wakeup()

    def unset(self):
        if self.timestamp:
            del self.timestamp
            if self in self.threadlet.timeouts:
                self.threadlet.debug('threadlet: %s: eventlet(%s): unset',
                                     self.threadlet.name,
                                     self.name)
                self.threadlet.timeouts.remove(self)
                self.threadlet.wakeup()

    def isset(self):
        return self in self.threadlet.timeouts

    def timeout(self, timestamp = None):
        if timestamp is None:
            timestamp = time.time()
        return self.timestamp - timestamp


class Tasklet(object):

    timestamp = None

    running = None
    suspended = False
    cancelled = False

    period = None
    jitter = None

    def __init__(self, threadlet, name, func):
        self.threadlet = threadlet
        self.name = name
        self.func = func

    def __repr__(self):
        return 'tasklet(%s)' % self.name

    def schedule(self, delay, jitter = None):
        if jitter is None:
            jitter = self.jitter
        if jitter:
            delay += delay * (random.random() - .5) * jitter
        self.schedule_at(time.time() + delay)

    def schedule_at(self, timestamp):
        if self in self.threadlet.timeouts:
            self.threadlet.timeouts.remove(self)
        self.threadlet.debug('threadlet: %s: tasklet(%s): schedule: dt=%.02f',
                             self.threadlet.name,
                             self.name,
                             timestamp - time.time())
        self.timestamp = timestamp
        if not (self.cancelled or self.suspended):
            self.threadlet.timeouts.add(self)
        self.threadlet.wakeup()

    def cancel(self):
        self.cancelled = True
        if self in self.threadlet.timeouts:
            self.threadlet.debug('threadlet: %s: tasklet(%s): cancel',
                                 self.threadlet.name,
                                 self.name)
            self.threadlet.timeouts.remove(self)
            self.threadlet.wakeup()
        del self.threadlet.tasklets[self.name]

    def suspend(self):
        if self.suspended:
            return
        self.suspended = True
        if self in self.threadlet.timeouts:
            self.threadlet.debug('threadlet: %s: tasklet(%s): suspend',
                                 self.threadlet.name,
                                 self.name)
            self.threadlet.timeouts.remove(self)
            self.threadlet.wakeup()

    def resume(self):
        if not self.suspended:
            return
        del self.suspended
        self.threadlet.debug('threadlet: %s: tasklet(%s): resume: timestamp=%.2f',
                             self.threadlet.name,
                             self.name,
                             self.timestamp - time.time())
        if not self.cancelled:
            self.threadlet.timeouts.add(self)
        self.threadlet.wakeup()

    def run(self):
        self.threadlet.debug("threadlet: %s: tasklet(%s): running", self.threadlet.name, self.name)
        self.running = True

        try:
            value = self.func(self)
            if isinstance(value, types.GeneratorType):
                value = yield from value
            self.threadlet.debug("threadlet: %s: tasklet(%s): done", self.threadlet.name, self.name)
        except asyncio.CancelledError:
            threadless.log.warn("threadlet: %s: tasklet(%s): cancelled", self.threadlet.name, self.name)
        except:
            threadless.log.exception("threadlet: %s: tasklet(%s): exception", self.threadlet.name, self.name)

        del self.running
        if self.suspended or self.cancelled:
            return
        if self.period and self not in self.threadlet.timeouts:
            self.schedule(self.period)


class Threadlet(object):

    future = None
    loop = None
    stopping = False

    _debug = False

    def __init__(self, name = None):
        self.name = name or 'Threadlet(%d)' % id(self)
        self.timeouts = set()
        self.signals = set()
        self.tasklets = {}

    def debug(self, *args):
        if self._debug:
            threadless.log.debug(*args)

    def eventlet(self, name):
        return Eventlet(self, name)

    def tasklet(self, name, start = None, period = None, jitter = None):
        assert name not in self.tasklets
        if start is None:
            start = time.time()
            if period and jitter:
                start += period * jitter * random.random()
        def _(func):
            task = self.tasklets[name] = Tasklet(self, name, func)
            task.period = period
            task.jitter = jitter
            task.schedule_at(start)
            return func
        return _

    def suspend(self, *taskNames):
        for name in taskNames:
            if name in self.tasklets:
                self.tasklets[name].suspend()

    def resume(self, *taskNames):
        for name in taskNames:
            if name in self.tasklets:
                self.tasklets[name].resume()

    def is_running(self, taskName):
        return self.tasklets[taskName].running

    def reschedule(self, taskName, delay = 0):
        self.reschedule_at(taskName, time.time() + delay)

    def reschedule_at(self, taskName, timestamp):
        self.tasklets[taskName].schedule_at(timestamp)

    def start(self, func = None, delay = 0, jitter = 0, when_done = None, wait = False):
        assert self.loop is None

        def default_func(thread):
            while not thread.stopping:
                yield from thread.idle()

        def default_done(future):
            if future.cancelled():
                threadless.log.warn("threadlet: %s: cancelled?", self.name)
            elif future.exception():
                exc = future.exception()
                threadless.log.warn("threadlet: %s: EXCEPTION: %s", self.name, str(exc))
            else:
                result = future.result()
                if result is not None:
                    threadless.log.warn("threadlet: %s: result: %r", self.name, result)

        def run():
            if delay:
                d = delay
                if jitter:
                    d += d * (random.random() - .5) * jitter
                # XXX make this interruptible on stop
                yield from asyncio.sleep(d)
            yield from (func or default_func)(self)

        def done(future):
            del self.loop
            self.timeouts.clear()
            self.signals.clear()

            try:
                (when_done or default_done)(future)
            except:
                threadless.log.exception("threadlet: %s: when-done", self.name)

        self.loop = asyncio.async(run())
        self.loop.add_done_callback(done)
        if wait:
            loop.run_until_complete(self.loop)

    def stop(self):
        self.stopping = True
        self.wakeup()

    def idle(self):
        """
        Sleep until one of the registered timeout expires
        """
        self.debug('threadlet: %s: idle: enter, signals=%r', self.name, self.signals)

        assert self.future is None

        # 1. check for expired timeouts
        def _expired():
            def sort_by_timeout(a, b):
                return cmp(a.timestamp, b.timestamp)
            expired = set()
            now = time.time()
            for timeout in sorted(self.timeouts, key=lambda x: x.timestamp):
                if timeout.timestamp > now:
                    return expired, timeout.timestamp
                expired.add(timeout)
            return expired, None

        while not self.stopping:

            expired, timestamp = _expired()

            while not (expired or self.signals):

                if timestamp is None:
                    delay = None
                else:
                    delay = max(0.0001, timestamp - time.time())

                self.future = asyncio.Future()
                try:
                    if delay:
                        self.debug('threadlet: %s: idle: sleep(dt=%.02f), signals=%r', self.name, delay, self.signals)
                        event = yield from asyncio.wait_for(self.future, delay)
                    else:
                        self.debug('threadlet: %s: idle: sleep, signals=%r', self.name, self.signals)
                        event = yield from self.future
                        self.debug('threadlet: %s: idle: wakeup, signals=%r', self.name, self.signals)
                except asyncio.TimeoutError:
                    self.debug('threadlet: %s: idle: timeout, signals=%r', self.name, self.signals)
                except asyncio.CancelledError:
                    self.debug('threadlet: %s: idle: cancelled, signals=%r', self.name, self.signals)
                finally:
                    if self.future:
                        del self.future

                if self.stopping:
                    break

                expired, timestamp = _expired()

            self.timeouts.difference_update(expired)

            events = set()
            for event in expired:
                if self.stopping:
                    break
                if isinstance(event, Tasklet):
                    if event.suspended:
                        self.debug('threadlet: %s: run-suspended: %s', self.name, event.name)
                        continue
                    if event.cancelled:
                        self.debug('threadlet: %s: run-cancelled: %s', self.name, event.name)
                        continue
                    yield from event.run()
                elif isinstance(event, Eventlet):
                    events.add(event)

            events.update(self.signals)
            self.signals.clear()

            self.debug('threadlet: %s: idle: leave signals=%r, events=%r', self.name, self.signals, events)

            if events:
                return events

        return set(['exit'])

    def signal(self, signal):
        self.debug('threadlet: %s: signal: %r', self.name, signal)
        self.signals.add(signal)
        self.wakeup()

    def wakeup(self):
        if self.future:
            future = self.future
            del self.future
            if future.done():
                if future.cancelled():
                    threadless.log.warn("threadlet: %s: wakeup: warning error=future-cancelled", self.name)
                elif future.exception():
                    threadless.log.warn("threadlet: %s: wakeup: warning error=future-exception, exception=%r", self.name, future.exception())
                else:
                    threadless.log.warn("threadlet: %s: wakeup: warning error=future-done, result=%r", self.name, future.result())
            else:
                future.set_result(None)


class APIException(Exception):
    pass

class APIError(APIException):
    pass

class APIHTTPError(APIException):
    def __init__(self, code, body):
        self.code = code
        self.body = body

def rest_query(url, data, method='POST', headers={}):
    o = urllib.parse.urlparse(url)
    host = o.hostname
    port = o.port
    path = o.path

    if port == None:
        if o.scheme == 'http':
            port = 80
        elif o.scheme == 'https':
            port = 443

    ssl = False
    if o.scheme == 'https':
        ssl = True

    if data:
        body = json.dumps(data)
    else:
        body = ""
    query = ("%s %s HTTP/1.1\r\n"
             "Host: %s\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %i\r\n"
             "Connection: close\r\n" % (method, path, host, len(body)))

    for header in headers:
        query += "%s: %s\r\n" % (header, headers[header])
    query += "\r\n%s" % (body, )

    try:
        reader, writer = yield from asyncio.open_connection(host = host,
                                                            port = port,
                                                            ssl = ssl)
        writer.write(query.encode())
    except asyncio.ConnectionRefusedError:
        raise APIError('connection refused')
    except asyncio.ConnectionResetError:
        raise APIError('connection reset while writing query')

    try:
        response = yield from reader.read()
    except asyncio.ConnectionResetError:
        raise APIError('connection reset while reading response')

    headers, content = response.decode().split("\r\n\r\n", 1)
    status = headers.split("\r\n")[0].split()[1]

    chunked = False
    for header in headers.split("\r\n"):
        if header.lower().startswith('transfer-encoding:') and "chunked" in header.lower():
            chunked = True
            break

    if chunked:
        data  = ""
        cdata = content
        while True:
            size, remain = cdata.split("\r\n", 1)
            if not size:
                break
            size = int(size, 16)
            if size == 0:
                break
            data += remain[:size]
            cdata = remain[size:]
        content = data

    if not status.startswith('2'):
        try:
            result = json.loads(content)
        except:
            result = content
        raise APIHTTPError(status, result)

    result = json.loads(content)

    return result


class Call(object):

    t_call = None
    t_run = None
    t_done = None

    def __init__(self, queue, url, params, timeout, method):
        self.queue = queue
        self.url = url
        self.params = params
        self.timeout = timeout
        self.method = method
        self.future = asyncio.Future()
        self.t_call = time.time()

    def callback(self, task):
        self.t_done = time.time()
        self.queue.dt_done.append(self.t_done - self.t_run)
        self.queue.running.remove(self)
        if self.future.done():
            threadless.log.info("callback for cancelled async call: %s", self.url)
            return

        exc = task.exception()
        if exc:
            # XXX intercept some
            self.future.set_exception(exc)
        else:
            self.future.set_result(task.result())

        self.queue.drain()

    def run(self):
        self.t_run = time.time()
        self.queue.running.add(self)
        self.queue.dt_run.append(self.t_run - self.t_call)
        uri = self.url
        asyncio.async(rest_query(uri, self.params, self.method)).add_done_callback(self.callback)


class CallQueue(object):

    n_call = 0
    n_max_pending = 0

    def __init__(self, name, running_max):
        self.name = name
        self.pending = collections.deque()
        self.running = set()
        self.running_max = running_max
        self.dt_run = []
        self.dt_done = []

    def cancel(self):
        while self.pending:
            self.pending.popleft().future.cancel()
        for a in self.running:
            a.future.cancel()

    def drain(self):
        if not self.pending:
            return
        if len(self.running) >= self.running_max:
            return
        self.pending.popleft().run()

    def call(self, url, params = {}, timeout = 60.0, method = 'POST'):
        self.n_call += 1
        a = Call(self, url, params, timeout, method)
        self.pending.append(a)
        self.n_max_pending = max(self.n_max_pending, len(self.pending))
        self.drain()
        return a.future

    def stats(self):
        def mean(l):
            if not l:
                return 0
            return sum(l) / len(l)
        res = { 'call': self.n_call,
                'pending': len(self.pending),
                'pending-max': self.n_max_pending,
                'running': len(self.running),

                'run': len(self.dt_run),
                'run-min': self.dt_run and min(self.dt_run) or 0,
                'run-max': self.dt_run and max(self.dt_run) or 0,
                'run-mean': mean(self.dt_run),

                'done': len(self.dt_done),
                'done-min': self.dt_done and min(self.dt_done) or 0,
                'done-max': self.dt_done and max(self.dt_done) or 0,
                'done-mean': mean(self.dt_done),
            }
        self.n_call = 0
        self.n_max_pending = len(self.pending)
        self.dt_run = []
        self.dt_done = []
        return res

    def log_stats(self, name = None, queue = None):
        s = self.stats()
        threadless.log.info("%s: event=call-queue, queue=%s, calls=%i, pending=%i, pending-max=%i, running=%i, run=%i(min=%.2f,max=%.2f,mean=%.2f), done=%i(min=%.2f,max=%.2f,mean=%.2f)",
                            name or 'async',
                            queue or self.name,
                            s['call'],
                            s['pending'],
                            s['pending-max'],
                            s['running'],
                            s['run'],
                            s['run-min'],
                            s['run-max'],
                            s['run-mean'],
                            s['done'],
                            s['done-min'],
                            s['done-max'],
                            s['done-mean'])

class RESTCaller(object):

    name = 'agent'
    q = None

    def __init__(self, name = None, running_max = None):
        if name:
            self.name = name
        if running_max:
            self.q = CallQueue(name, running_max)

    def call(self, url, params = None, method = 'POST'):
        try:
            res = yield from self.q.call(url, params, method = method)
        except APIError as exc:
            threadless.log.warn('%s: api-error, url=%s, error=%s', self.name, url, exc)
            raise
        except APIHTTPError as exc:
            threadless.log.warn('%s: api-http-error, url=%s, code=%s, error=%s', self.name, url, exc.code, exc.body)
            raise
        except:
            threadless.log.exception('%s: url=%s', self.name, url)
            raise
        return res

    def get(self, url):
        return self.call(url, method = 'GET')

    def post(self, url, params = None):
        return self.call(url, params = params, method = 'POST')
