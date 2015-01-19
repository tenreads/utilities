#! coding=utf-8
__author__ = "Sriram Murali Velamur"
__all__ = ("ProcessManager",)

import sys
sys.dont_write_bytecode = True
from threading import Thread
from multiprocessing import cpu_count, Process, Pool, Queue
from types import FunctionType
from time import sleep


class CustomProcess(Process):

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, queue=None):
        super(CustomProcess, self).__init__(group, target, name, args, kwargs)
        self.queue = queue

    def run(self):
        response = self._target(*self._args, **self._kwargs)
        self.queue.put(response)


class ProcessManager(object):

    def __init__(self, processes=None):
        _count = cpu_count()
        self.process_count = processes if isinstance(processes, int) \
            and 0 < processes <_count else _count
        self.processes = []
        self.buffer = []
        self.queue = Queue()
        self.callbacks = {}

    def __repr__(self):
        return "<ProcessManager - Slot size[%s] - Added[%s]>" % \
            (self.process_count, len(self.processes) + len(self.buffer))

    def __str__(self):
        return "<ProcessManager - Slot size[%s] - Added[%s]>" % \
            (self.process_count, len(self.processes) + len(self.buffer))

    def add(self, handler, args=None, kwargs=None, callback=None):
        handler = handler if isinstance(handler, FunctionType) else None
        args = args if isinstance(args, tuple) else ()
        kwargs = kwargs if isinstance(kwargs, dict) else {}
        callback = callback if isinstance(callback, FunctionType) else None

        if handler:
            process = CustomProcess(target=handler, args=args, kwargs=kwargs, queue=self.queue)
            if len(self.processes) < self.process_count:
                # add process
                self.processes.append(process)
            else:
                self.buffer.append(process)

            if callback:
                self.callbacks[process] = callback

    def start(self):
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