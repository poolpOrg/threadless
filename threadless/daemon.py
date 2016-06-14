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

import atexit
import contextlib
import errno
import grp
import json
import os
import pwd
import signal
import socket
import subprocess
import sys
import time

import threading
import multiprocessing

import threadless.log

DAEMONIZE = 1
USERNAME  = "_threadless"
PIDFILE = None

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(name):
        pass

def _mkdir_p(*path):
    try:
        os.makedirs(os.path.join(*path))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

class Daemon(object):
    """
    A generic daemon class.

    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self,
                 procname,
                 username = None,
                 syslog = True,
                 pidfile = None,
                 stdout = None,
                 stderr = None):
        self.procname = procname
        self.syslog = syslog
        self.pidfile = pidfile
        self.username = username
        self.stdout = stdout
        self.stderr = stderr

    def _open_log(self, syslog = None, debug = None):
        threadless.log.setup(self.procname, debug = debug)

    def _write_pid(self):
        pid = self.get_pid()
        if pid is not None:
            if self.is_running(pid):
                message = "Error: process \"%s\" is already running with pid %i\n"
                sys.stderr.write(message % (self.procname, pid))
                sys.exit(1)
            else:
                message = "Warning: overwriting stale pidfile %s with pid %i\n"
                sys.stderr.write(message % (self.pidfile, pid))

        def _del_pid():
            os.remove(self.pidfile)
        atexit.register(_del_pid)
        _mkdir_p(os.path.dirname(self.pidfile))
        open(self.pidfile, 'w').write("%i\n" % os.getpid())

    def _drop_priv(self):
        if os.getuid() != 0:
            return
        groups = list(set([ g.gr_gid for g in grp.getgrall() if self.pw.pw_name in g.gr_mem ] + [ self.pw.pw_gid]))
        os.setgroups(groups)
        os.setresgid(self.pw.pw_gid, self.pw.pw_gid, self.pw.pw_gid)
        os.setresuid(self.pw.pw_uid, self.pw.pw_uid, self.pw.pw_uid)

    def _daemonize(self):
        if DAEMONIZE == 2:
            return
        try:
            pid = os.fork() 
            if pid > 0:
                sys.exit(0) 
        except OSError as e: 
            sys.stderr.write("Cannot fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        os.setsid() 
        #os.chdir("/") 
        #os.umask(0) 

        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                sys.exit(0) 
        except OSError as e: 
            sys.stderr.write("Cannot fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 

        if self.stdout:
            self._stdout = IO(self.stdout, sys.stdout)
            self._stdout.open()
        if self.stderr:
            self._stderr = IO(self.stderr, sys.stderr)
            self._stderr.open()

        def _hangup(signum, frame):
            if self.stdout:
                self._stdout.hangup()
            if self.stderr:
                self._stderr.hangup()
        signal.signal(signal.SIGHUP, _hangup)

        def _handler(signum, frame):
            if self.terminate:
                self.terminate()
            else:
                threadless.log.info("Got signal %i. Exiting", signum)
                sys.exit(0)
        signal.signal(signal.SIGTERM, _handler)

    def _start(self, foreground = True):
        if self.username is None:
            if os.getuid() == 0:
                sys.stderr.write("Refusing to run as superuser\n")
                sys.exit(1)
            self.pw = pwd.getpwuid(os.getuid())
        else:
            self.pw = pwd.getpwnam(self.username)
            if os.getuid() not in (0, self.pw.pw_uid):
                sys.stderr.write("Cannot run as user \"%s\"\n" % (self.username, ))
                sys.exit(1)

        setproctitle(self.procname)

        if not foreground:
            self._drop_priv()
            self.pre_daemonize()
            self._daemonize()
            if self.pidfile:
                self._write_pid()
            self._open_log(syslog = self.syslog)
        else:
            self._drop_priv()
            self.pre_daemonize()
            self._open_log(syslog = False, debug = True)

        self.run()


    setup = None
    def pre_daemonize(self):
        if self.setup:
            self.setup()

    def get_pid(self):
        try:
            return int(open(self.pidfile).read().strip())
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                return
            raise

    def is_running(self, pid = None):
        if pid is None:
            pid = self.get_pid()
        if pid is None:
            return False
        procname = self.procname

        for line in os.popen("ps auxwww").read().split("\n"):
            parts = line.split()
            if not parts:
                continue
            if parts[0] != self.username:
                continue
            try:
                ppid = int(parts[1])
            except:
                continue
            if ppid != pid:
                continue
            if procname in parts or self.procname in parts:
                return True

        return False

    def status(self):
        return self.is_running() is not None

    def _kill(self, sig, wait = None):
        pid = self.get_pid()
        if pid is None:
            message = "Process \"%s\" is not running\n"
            sys.stderr.write(message % (self.procname))
            return

        if not self.is_running(pid):
            message = "Process \"%s\" is not running (stale pidfile %s with pid %i)\n"
            sys.stderr.write(message % (self.procname, self.pidfile, pid))
            return

        os.kill(pid, sig)
        t0 = time.time()
        if wait:
            while(self.is_running(pid)):
                if time.time() - t0 >= 10:
                    message = "Warning: process \"%s\" is still running\n"
                    sys.exit(1)
                time.sleep(.1)

    def stop(self):
        self._kill(signal.SIGTERM, wait = True)

    def kill(self):
        self._kill(signal.SIGKILL)

    def hangup(self):
        self._kill(signal.SIGHUP)

    def start(self, start, stop = None, setup = None, foreground = None):
        if foreground is None:
            foreground = not DAEMONIZE

        if self.pidfile and self.is_running():
            message = "Error: process \"%s\" is already running with pid %i\n"
            sys.stderr.write(message % (self.procname, self.get_pid()))
            sys.exit(1)

        self.run = start
        self.setup = setup
        self.terminate = stop
        self._start(foreground = foreground)

def daemon(*args, **kwargs):
    if "username" not in kwargs:
        kwargs["username"] = USERNAME
    if "pidfile" in kwargs and PIDFILE is not None:
        # override
        kwargs["pidfile"] = PIDFILE
    return Daemon(*args, **kwargs)


class IO(object):

    def __init__(self, path, stream):
        self.path = path
        self.stream = stream

    def open(self, msg = 'open'):
        with open(self.path, 'a+') as f:
            os.dup2(f.fileno(), self.stream.fileno())
        self.stream.seek(0, os.SEEK_END)
        self.stream.write("%s [%i]: --- %s ---\n" % (time.ctime(), os.getpid(), msg))
        self.stream.flush()

    def hangup(self):
        self.open('rotate')


if __name__ == "__main__":
    class Foobar():
        def start(self):
            while True:
                time.sleep(1)
        def stop(self): pass
    foo = Foobar()

    d = daemon(procname = "tester",
               username = "gilles",
               stderr = "/tmp/tester.err",
               pidfile = "/tmp/tester.pid")


    if sys.argv[1] == "start":
        d.start(foo.start, foo.stop)
    if sys.argv[1] == "stop":
        d.stop()
