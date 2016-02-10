#!/bin/env python

import argparse
import numpy as np
import os
from taq import raw_taq
from zipfile import ZipFile

class InFile(object):

    def __init__(self, fp: str):
        self.generator = raw_taq.TAQ2Chunks(fp, do_process_chunk=True, chunksize=1000)

    def __next__(self):
        return self.generator.__next__()

class OutFile(object):

    def __init__(self, fp: str):
        self.f = fp
        if os.path.isfile(self.f):
            raise OSError("File already exists")
        with ZipFile(self.f, 'w') as f:
            f.writestr(self.f, b'')

    def write(self, data):
        bytes_data = b''
        for row in data:
            bytes_data += row.tostring() + b'\n'
        with ZipFile(self.f, 'a') as f:
            f.writestr(self.f, bytes_data)

class Sanitizer(object):

    def __init__(self):
        self.fake_exchanges = [b'P', b'Y', b'K', b'T', b'M']
        self.fake_symbol_roots = [b'CMUIQR',
                            b'QNZJTK',
                            b'DJCLZC',
                            b'ENKZDV',
                            b'NEWPHK']
        self.fake_symbol_suffixes = [b'WZNAPJDEBJ'
                                b'HURHRDTGZO'
                                b'IZCJQKGBAG'
                                b'YLZCHBOQWO'
                                b'GVPTQGJZOP']

    def sanitize(self, data):
        for ix in range(0,len(data)):
            data[ix][4] = self.fake_exchanges[np.random.randint(0,len(self.fake_exchanges))]
            data[ix][5] = self.fake_symbol_roots[np.random.randint(0,len(self.fake_symbol_roots))]
            data[ix][6] = self.fake_symbol_suffixes[np.random.randint(0,len(self.fake_symbol_suffixes))]
        return data


    def run(self, fp_in: str, fp_out: str, size: int):
        self.remaining_chunks = size
        generator = InFile(fp_in)
        writer = OutFile(fp_out)
        while self.remaining_chunks > 0:
            self.remaining_chunks -= 1
            data = next(generator)
            data = self.sanitize(data)
            writer.write(data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file_in', help="Path to Zipped TAQ data")
    parser.add_argument('file_out', default='test_data_public.zip' ,help="Path to write output")
    parser.add_argument('size', type=int, help="Integer of 1000 row chunks to anonymize")
    args = parser.parse_args()

    Sanitizer().run(fp_in=args.file_in, fp_out=args.file_out, size=args.size)
