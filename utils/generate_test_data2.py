'''Mix up symbols and times a little bit'''

from zipfile import ZipFile
from string import ascii_uppercase
from random import sample

import taq
import taq.processing as tp


class Sanitizer:
    '''Take a TAQ file and make it fake while preserving structure'''

    symbol_column = 'Symbol_root'

    # This will preserve the fake symbol across chunks
    symbol_map = {}
    ascii_bytes = ascii_uppercase.encode('ascii')

    def __init__(self, fname_in, fname_out):
        taq_in = taq.TAQ2Chunks(fname_in, do_process_chunk=False)
        self.iterator = tp.split_chunks(taq_in, self.columns)

    def __enter__(self):
        # Currently don't have a nice context manager for TAQ2Chunks
        # XXX do something here to open our output ZipFile
        # self.f = open(self.path, 'ab')
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        # self.f.close()
        pass

    def process(self, size):
        for chunk in self.iterator:
            # XXX a little annoying... and undocumented that split makes
            # thing unwriteable. Should double-check.
            chunk.flags.writeable = True
            self.fake_symbol_replace(chunk, self.symbol_column)
            self.fudge_up(chunk, self.time_col, self.max_time)
            self.write_chunk(chunk)

    def fake_symbol_replace(self, chunk, column):
        '''Make a new fake symbol if we don't have it yet, and return it'''
        real_symbol = chunk[self.column][0]
        new_fake_symbol = ''.join(sample(self.ascii_bytes, len(real_symbol)))
        fake_symbol = self.symbol_map.setdefault(real_symbol, new_fake_symbol)

        chunk[self.column] = fake_symbol

    def fudge_up(self, chunk, column, max_value):
        '''Increase each entry in column by some random increment.

        Make sure the values stay monotonic, and don't get bigger than
        max_value.'''
        pass

    def write_chunk(self, chunk):
        '''Write the chunk to the already-open zipfile'''
        pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('fname_in', help="Path to Zipped TAQ data")
    parser.add_argument('fname_out', default='test_data_public',
                        help="Path to write output"
                             "(both zip archive and contained file")
    parser.add_argument('size', type=int,
                        help="Integer number of lines to sanitize and write")
    args = parser.parse_args()

    Sanitizer(args.fname_in, args.fname_out).process(args.size)
