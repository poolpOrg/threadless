#
# Copyright (c) 2016 Gilles Chehade <gilles@poolp.org>
# Copyright (c) 2013 Eric Faurot <eric@faurot.net>
# 
# Permission to use, copy, modify, and/or distribute this software for any
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

import socket
import time

import threadless.async
import threadless.daemon
import threadless.log


class Agent(object):

    configured = False
    
    def __init__(self):
        self.thread = threadless.async.Threadlet("agent")

    def uptime(self):
        return time.time() - self.start_time

    def hostname(self):
        return socket.getaddrinfo(socket.gethostname(), 0, 0, 0, 0,
                                  socket.AI_CANONNAME)[0][3]

    def thread_loop(self, thread):
        pass

    ##
    ## Daemon interface
    ##
    def start(self):
        self.start_time = time.time()
        threadless.log.info("agent: started")
        self.thread.start(func = self.parent_thread, wait = True)
        threadless.log.info("agent: stopped")

    def stop(self):
        if self.thread.stopping:
            return
        threadless.log.info("agent: stopping")
        self.thread.stop()

    def parent_thread(self, thread):

        yield self.thread_loop(thread)

        while not thread.stopping:
            yield from thread.idle()

        yield from threadless.async.asyncio.sleep(.5)
