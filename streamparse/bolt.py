"""Minimal shim for streamparse Bolt used for local testing and harness.
This provides a tiny `Bolt` base class with an `emit` method so existing
bolt implementations can be imported and used in-process without installing
the real `streamparse` package.
"""
class Bolt:
    @classmethod
    def spec(cls, **kwargs):
        return {"component": cls.__name__, "kwargs": kwargs}

    def initialize(self, conf, ctx):
        return None

    def process(self, tup):
        raise NotImplementedError()

    def emit(self, values):
        # default emit: append to self._emitted if available, else print
        try:
            lst = getattr(self, '_emitted', None)
            if lst is None:
                self._emitted = []
                lst = self._emitted
            lst.append(values)
        except Exception:
            print('EMIT:', values)
