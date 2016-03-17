'''Tools for operating on chunks of financial time-series data.

'''

import numpy as np


def split_chunks(iterator_in, columns):
    '''Split a chunk based on a list of columns

    Note that if the next chunk exhibits the continuation of a symbol, this
    will NOT combine derived chunks for the same symbol.'''

    for chunk in iterator_in:
        unique_symbols, start_indices = \
            np.unique(chunk[columns], return_index=True)
        # This takes up a trivial amount of memory, due to the use of views
        # And of course we don't want to split on the first index, it's 0
        for split_c in np.split(chunk, start_indices[1:]):
            yield split_c


def joined_chunks(iterator_in, columns, row_limit=np.inf):
    '''If a chunk matches the columns from a previous chunk, concatenate!

    The logic only inspects the first record. `row_limit` is provided to help
    ensure memory limits. But is NOT a limit on records in memory (you can have
    about the row_limit + size of the base chunks coming off disk)'''

    to_join = []
    total_len = 0

    for chunk in iterator_in:
        # Something is in to_join and (we could blow our row_limit or we have a
        # mis-match)
        if to_join and (total_len + len(chunk) > row_limit or
                        to_join[0][columns][0] != chunk[columns][0]):
            yield np.hstack(to_join)
            to_join = [chunk]
            total_len = len(chunk)
        # Nothing there yet or we have a match and space to join
        else:
            to_join.append(chunk)
            total_len += len(chunk)

    # Get our last chunk
    yield np.hstack(to_join)


class ProcessChunk:
    '''An abstract base class for processing chunks.

    As it stands, a class-based structure is unnecessary (see the
    straightforward generator functions above). But this gives the idea for
    something with a bit more flexibility.

    Probably we should be using Dask or Blaze or something. Next step, maybe?
    '''

    def __init__(self, *args, **kwargs):
        '''Initialize a derived iterator.

        See the _process_chunks method for arguments.'''

        self.iterator = self._process_chunks(iterator_in, columns)

    def __iter__(self):
        # Returning the internal iterator avoids a function call, not a big
        # deal, but may as well avoid extra computation
        return self.iterator

    def __next__(self):
        return next(self.iterator)

    def _process_chunks(self, *args, **kwargs):
        raise NotImplementedError('Please subclass ProcessChunk')

