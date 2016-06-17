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

import logging
import logging.config
import traceback
import socket

runinfo = {}

_logger = logging.getLogger("threadless")

def debug(*args):
    try:
        _logger.debug(*args)
    except:
        error('LOG DEBUG FAILURE: %r', args)
        raise

def info(*args):
    try:
        _logger.info(*args)
    except:
        error('LOG INFO FAILURE: %r', args)
        raise

def warn(*args):
    try:
        _logger.warn(*args)
    except:
        error('LOG WARN FAILURE: %r', args)
        raise

def error(*args):
    try:
        _logger.error(*args)
    except:
        error('LOG ERROR FAILURE: %r', args)
        raise

def exception(*args):
    trace = traceback.format_exc()
    if args:
        try:
            ctx = args[0] % tuple(args[1:])
        except:
            ctx = "???" + repr(args)
        trace = ctx + "\n---\n" + trace
        info(trace)

def future(future, *args):
    if future.exception():
        try:
            raise future.exception()
        except:
            exception(*args)

def setup(procname, debug = False):
    # XXX chicken egg
    runinfo['service'] = procname
    runinfo['host'] = socket.getaddrinfo(socket.gethostname(), 0, 0, 0, 0, socket.AI_CANONNAME)[0][3]
    runinfo['debug'] = debug

    logging.config.dictConfig({
            "version": 1,
            "formatters": {
                "default": {
                    "format": " ".join([ "%s[%%(process)s]:" % (procname, ),
                                         "%(levelname)s:",
                                         "%(message)s" ]),
                    "datefmt": "%Y/%m/%d %H:%M:%S",
                    },
                "exception": {
                    "format": "".join([ "-" * 72 + "\n",
                                        "%(asctime)s",
                                        " %s[%%(process)s]:" % (procname, ),
                                        " %(levelname)s:",
                                        " %(message)s",
                                        ]),
                    "datefmt": "%Y/%m/%d %H:%M:%S",
                    },
                },
            "handlers": {
                "syslog": {
                    "class": "logging.handlers.SysLogHandler",
                    "formatter": "default",
                    "level": debug and "DEBUG" or "INFO",
                    "address": "/dev/log",
                    },
                "stdout": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "level": debug and "DEBUG" or "INFO",
                    },
                "null": {
                    "class" : "logging.NullHandler",
                    "formatter": "exception",
                    }
                },
            "loggers": {
                "threadless": {
                    "handlers" : [ debug and "stdout" or "syslog" ],
                    "level": debug and "DEBUG" or "INFO",
                    "propagate": False,
                    },
                },
            "root": {
                "handlers": [ debug and "stdout" or "syslog" ],
                "level": "INFO",
                }
            })

