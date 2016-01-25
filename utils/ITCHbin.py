#!/usr/bin/env python3

import sys
import struct
import gzip

with gzip.GzipFile(sys.argv[1]) as infile:
    while(True):
        rec_len, = struct.unpack('>h', infile.read(2))
        print(rec_len)
        rec = infile.read(rec_len)
        if rec.startswith(b'S'):
            print(struct.unpack('>2h', rec[1:5]),
                  int.from_bytes(rec[5:11], 'big'),
                  chr(rec[11]))
