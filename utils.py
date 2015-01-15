#! coding=utf-8
__author__ = "Sriram Murali Velamur<sriram@likewyss.com>"
__all__ = ("ProcessManager",)


import sys
sys.dont_write_bytecode = True
from multiprocessing import Process, cpu_count
from time import sleep
import random


class ProcessManager(object):
    def __init__(self, slot_size=cpu_count()):
        _count = cpu_count()
        self.slot_size = slot_size if isinstance(slot_size, int) and \
            slot_size < _count else _count
        self.slots = []
        self.buffer = []
        self.callback_map = {}
        self.started = False

    def add(self, handler, args=None, kwargs=None, callback=None):

        args = args or ()
        kwargs = kwargs or {}

        _process = Process(target=handler, args=args, kwargs=kwargs)
        if len(self.slots) < self.slot_size:
            self.slots.append(_process)
        else:
            self.buffer.append(_process)
        self.callback_map[_process] = callback

    def start(self):

        self.started = True
        [item.start() for item in self.slots if not item.is_alive()
         and item._popen is None]

        while 1:
            for item in self.slots:
                if item._popen and not item.is_alive():
                    self.slots.remove(item)
                    item.terminate()
                    _callback = self.callback_map.get(item)
                    if _callback:
                        _callback()

            if len(self.slots) < self.slot_size:
                if self.buffer:
                    _item = self.buffer[0]
                    self.slots.append(_item)
                    self.buffer.remove(_item)
                    _item.start()

            if not self.slots and not self.buffer:
                break
            else:
                sleep(0.001)

    def apply(self, iterable_call):
        [self.add(*item) for item in iterable_call]
        if not self.started:
            self.start()


if __name__ == '__main__':

    def test(x):
        return (a**2 for a in x)


    manager = ProcessManager()
    manager.apply(
        [(test, (range(1, random.randint(1, 10000)),))
         for x in range(1, 20)])
