#! coding=utf-8
__author__ = "Sriram Murali Velamur"
__all__ = ("ProcessManager",)

import sys
sys.dont_write_bytecode = True
from multiprocessing import cpu_count, Process, Pool, Queue
from types import FunctionType
from time import sleep


class CustomProcess(Process):
    """
    Custom process class. Adds a queue parameter to respond back to.
    """


    def __init__(self, group=None, target=None,
                 name=None, args=(), kwargs={}, queue=None):
        """
        CustomProcess class init.
        Adds an additional queue parameter
        """
        super(CustomProcess, self).__init__(group, target,
                                            name, args, kwargs)
        self.queue = queue

    def run(self):
        """
        CustomProcess run method.
        Calls the base run flow, captures response, and puts it into
        the instance queue.
        """
        response = self._target(*self._args, **self._kwargs)
        self.queue.put(response)


class ProcessManager(object):
    """ProcessManager class"""

    def __init__(self, processes=None):
        """
        ProcessManager class init.
        Number of processes restricted to CPU count.
        Defaults to CPU count if None.
        """
        _count = cpu_count()
        self.process_count = processes if isinstance(processes, int) \
            and 0 < processes <_count else _count
        self.processes = []
        self.buffer = []
        self.queue = Queue()
        self.callbacks = {}

    def __repr__(self):
        _added = len(self.processes) + len(self.buffer)
        return "<ProcessManager - Slot size[%s] - Added[%s]>" % \
            (self.process_count, _added)

    def __str__(self):
        _added = len(self.processes) + len(self.buffer)
        return "<ProcessManager - Slot size[%s] - Added[%s]>" % \
            (self.process_count, _added)

    def add(self, handler, args=None, kwargs=None, callback=None):
        """
        Adds a handler method to the ProcessManager processes slot.
        If slot is full, adds to buffer which is pulled in when another
        process finishes.
        If a callback is provided, registers it in self.callbacks
        """
        handler = handler if isinstance(handler, FunctionType) else None
        args = args if isinstance(args, tuple) else ()
        kwargs = kwargs if isinstance(kwargs, dict) else {}
        callback = callback if isinstance(callback, FunctionType) else None

        if handler:
            process = CustomProcess(target=handler,
                                    args=args,
                                    kwargs=kwargs,
                                    queue=self.queue)
            if len(self.processes) < self.process_count:
                # add process
                self.processes.append(process)
            else:
                self.buffer.append(process)

            if callback:
                self.callbacks[process] = callback

    def start(self):
        """
        ProcessManager start.
        Starts all entries in self.processes slot and watches for
        completion. If a completed process has a registered callback,
        reads from queue and applies the result on the callback before
        removing it from the processes slot.
        Moves a new waitlisted process from the buffer slot and starts it.
        """
        for item in self.processes:
            item.start()
        while 1:
            if not self.processes and not self.buffer:
                break
            else:
                for item in self.processes:
                    if not item.is_alive():
                        _cb = self.callbacks.get(item)
                        if _cb:
                            _cb_val = self.queue.get()
                            _cb(_cb_val)
                        item.terminate()
                        if item in self.processes:
                            self.processes.remove(item)
                        if self.buffer:
                            _item = self.buffer[0]
                            self.processes.append(_item)
                            self.buffer.remove(_item)
                            _item.start()
