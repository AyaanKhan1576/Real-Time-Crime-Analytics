"""Minimal Bolt shim for local testing and in-process topology harness.
This provides a `Bolt` base class with emit() so bolt code can run
without installing the real streamparse package during development/testing.
"""


class Bolt:
    def initialize(self, conf, ctx):
        return None

    def process(self, tup):
        raise NotImplementedError()

    def emit(self, values):
        # Capture emits for in-process runners; in real Storm these go to the topology
        try:
            lst = getattr(self, '_emitted', None)
            if lst is None:
                self._emitted = []
                lst = self._emitted
            lst.append(values)
        except Exception:
            print('EMIT:', values)
