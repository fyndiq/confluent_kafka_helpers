"""
Default atexit registered functions will not be triggered on signals.

However - if we manually handle the signals and exit the functions will be
triggered.
"""
import signal
import sys


def handler(signum, frame):
    sys.exit(128 + signum)


signal.signal(signal.SIGTERM, handler)
