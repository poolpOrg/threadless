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
import random
import selectors
import time
import types

selector = selectors.SelectSelector()
loop = asyncio.SelectorEventLoop(selector)
asyncio.set_event_loop(loop)
loop = asyncio.get_event_loop()

import threadless.log

class Timeout(Exception):
    pass


_task_cancel = object()

_task_suspend = object()


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

    #def elapsed(self, offset = 0):
    #    return self.timestamp + offset < time.time()

class Tasklet(object):

    timestamp = None

    running = None
    suspended = False

    period = None
    jitter = None
    on_cancel = None
    on_timeout = None
    on_exception = None

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
        return self.schedule_at(time.time() + delay)

    def schedule_at(self, timestamp):
        if self.running:
            return timestamp
        if self in self.threadlet.timeouts:
            self.threadlet.timeouts.remove(self)
        self.threadlet.debug('threadlet: %s: tasklet(%s): schedule: dt=%.02f',
                             self.threadlet.name,
                             self.name,
                             timestamp - time.time())
        self.timestamp = timestamp
        self.threadlet.timeouts.add(self)
        self.threadlet.wakeup()

    def cancel(self):
        if self.running:
            return _task_cancel
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
        if self.running:
            return _task_suspend
        if self in self.threadlet.timeouts:
            self.threadlet.debug('threadlet: %s: tasklet(%s): suspend',
                                 self.threadlet.name,
                                 self.name)
            self.threadlet.timeouts.remove(self)
            self.threadlet.wakeup()

    def resume(self):
        if not self.suspended:
            #threadless.log.warn("threadlet: %s: tasklet(%s): resume: warning error=not-suspended", self.name)
            return
        del self.suspended
        self.threadlet.debug('threadlet: %s: tasklet(%s): resume: timestamp=%.2f',
                             self.threadlet.name,
                             self.name,
                             self.timestamp - time.time())
        self.threadlet.timeouts.add(self)
        self.threadlet.wakeup()

    def run(self):

        def _reschedule(when, relative):
            del self.running
            if when is _task_suspend:
                pass
            elif when is _task_cancel:
                del self.threadlet.tasklets[self.name]
            elif when is None:
                if self.period:
                    self.schedule(self.period)
            else:
                self.schedule_at(when + relative)

        try:
            self.threadlet.debug("threadlet: %s: tasklet(%s): running", self.threadlet.name, self.name)
            self.running = True
            value = self.func(self)
            if isinstance(value, types.GeneratorType):
                value = yield from value
            self.threadlet.debug("threadlet: %s: tasklet(%s): done", self.threadlet.name, self.name)
            _reschedule(value, 0)
        except asyncio.CancelledError:
            threadless.log.warn("threadlet: %s: tasklet(%s): cancelled", self.threadlet.name, self.name)
            _reschedule(self.on_cancel, time.time())
        except:
            threadless.log.exception("threadlet: %s: tasklet(%s): exception", self.threadlet.name, self.name)
            _reschedule(self.on_exception, time.time())


def default_main(thread):
    while not thread.stopping:
        yield from thread.idle()


class Threadlet(object):

    future = None
    loop = None
    sleeping = False
    stopping = False

    _debug = False
    _started = 0

    def __init__(self, name, main = default_main):
        self.name = name
        self.main = main
        self.timeouts = set()
        self.signals = set()
        self.tasklets = {}

    def debug(self, *args):
        if self._debug:
            threadless.log.info(*args)

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

    def _main_loop(self, delay, jitter):

        self.debug("threadlet: %s: starting", self.name)

        if delay:
            try:
                self.sleeping = True
                yield from asyncio.sleep(delay + jitter * random.random())
                del self.sleeping
            except asyncio.CancelledError:
                threadless.log.debug("threadlet: %s: sleep interrupted", self.name)

        yield from self.main(self)

    def start(self, delay = 0, jitter = 0, when_done = None):
        assert self.loop is None

        def done(future):
            Threadlet._started -= 1
            del self.main
            del self.loop
            self.timeouts.clear()
            self.signals.clear()
            
            if when_done is not None:
                try:
                    when_done(future)
                except:
                    threadless.log.exception("threadlet: %s: when-done", self.name)
                return

            if future.cancelled():
                threadless.log.warn("threadlet: %s: cancelled?", self.name)
            elif future.exception():
                exc = future.exception()
                threadless.log.warn("threadlet: %s: EXCEPTION: %s", self.name, str(exc))
            else:
                result = future.result()
                if result is not None:
                    threadless.log.warn("threadlet: %s: result: %r", self.name, result)

        Threadlet._started += 1
        self.loop = asyncio.async(self._main_loop(delay, jitter))
        self.loop.add_done_callback(done)
        return self.loop

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

    def wakeup(self, result = None):
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
                future.set_result(result)
