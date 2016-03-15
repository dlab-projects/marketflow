'''A set of utilities for financial data analysis'''

from time import time


# Benchmarking


def timeit(method):
    '''Return a function that behaves the same, except it prints timing stats.

    Lightly modified from Andreas Jung. Unlicensed, but simple enough it should
    not be a license issue:
        https://www.andreas-jung.com/contents/a-python-decorator-for-measuring-the-execution-time-of-methods
    '''
    def timed(*args, **kw):
        ts = time()
        result = method(*args, **kw)
        te = time()

        print('{!r} ({!r}, {!r}): {:.3} sec'.format(
            method.__name__, args, kw, te-ts))
        return result

    return timed
