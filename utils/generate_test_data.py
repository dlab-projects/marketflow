#!/bin/env python

import os
import random
import string

class InFile(object):

    def __init__(self, fp: str, num_lines: int):
        self.path = fp
        self.limit = num_lines

    def __enter__(self):
        self.f = open(self.path, 'rb')
        self.current_line = ''
        self.next_index = 0
        self.__next__()

    def __exit__(self, exception_type, exception_value, traceback):
        self.f.close()

    def __iter__(self):
        return self

    def __next__(self):
        if self.next_index == self.limit:
            raise StopIteration
        else:
            self.current_line = self.f.readline()
            if not self.current_line:
                raise StopIteration
            self.next_index += 1
            if len(self.current_line) == 99:
                return self.current_line[1:]
            else:
                return self.current_line

class OutFile(object):

    def __init__(self, fp: str):
        self.path = fp

    def __enter__(self):
        with open(self.path, 'wb') as f:
            f.write(b'')
        self.f = open(self.path, 'ab')

    def __exit__(self, exception_type, exception_value, traceback):
        self.f.close()

    def write(self, data):
        if not isinstance(data, bytes):
            try:
                data = data.encode('utf-8')
            except UnicodeEncodeError:
                try:
                    data = data.encode('windows-1252')
                except UnicodeEncodeError:
                    raise
        self.f.write(data)

class Sanitizer(object):

    def __init__(self):
        self.fake_exchanges = [b'P', b'Y', b'K', b'T', b'M']

    def input_line(self, line):
        if not isinstance(line, bytes):
            raise TypeError("Expected bytes object, found {}".format(type(line)))
        if len(line) not in [98]:
            raise IndexError("""
            Expected object of length 98, not {}
            Did you strip out the leading whitespace?
            """.format(len(line)))
        self.line = line

    def fudge_timestamp(self):
        new_bytes = ''.join(random.sample(string.digits, 3)).encode('ascii')
        self.replace_bytes(new_bytes, 6, 9)

    def fudge_exchanges(self):
        self.replace_bytes(random.choice(self.fake_exchanges), 9, 10)
        self.replace_bytes(random.choice(self.fake_exchanges), 67, 68)
        self.replace_bytes(random.choice(self.fake_exchanges), 68, 69)

    def fudge_symbol(self):
        if not self.line[10:16].isspace():
            new_bytes = ''.join(random.sample(string.ascii_uppercase, 6)).encode('ascii')
            self.replace_bytes(new_bytes, 10, 16)

    def fudge_suffix(self):
        if not self.line[16:26].isspace():
            new_bytes = ''.join(random.sample(string.ascii_uppercase, 10)).encode('ascii')
            self.replace_bytes(new_bytes, 16, 26)

    def sanitize(self):
        self.fudge_timestamp()
        self.fudge_exchanges()
        self.fudge_symbol()
        self.fudge_suffix()
        return self.line

    def replace_bytes(self, new_bytes, start_ix, stop_ix):
        if len(new_bytes) != stop_ix - start_ix:
            raise IndexError("Replacement is not same length")
        self.line = self.line[:start_ix] + new_bytes + self.line[stop_ix:]

    def run(self, fp_in: str, fp_out: str, num_lines: int):
        o = OutFile(fp_out)
        with o:
            i = InFile(fp_in, num_lines)
            with i:
                o.write(i.current_line)
                for line in i:
                    self.input_line(line)
                    o.write(self.sanitize())

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('file_in', help="Path to Zipped TAQ data")
    parser.add_argument('file_out', default='test_data_public' ,help="Path to write output (not zipped)")
    parser.add_argument('size', type=int, help="Integer of number of lines to sanitize and write")
    args = parser.parse_args()

    Sanitizer().run(fp_in=args.file_in, fp_out=args.file_out, num_lines=args.size)
