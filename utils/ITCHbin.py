#!/usr/bin/env python3

import sys
import struct
import gzip

# Currently, we redundantly unpack and keep the record type indicator

# Struct documented here: https://docs.python.org/3.4/library/struct.html

# ITCH record formats detailed in section 4.1 here:
# http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf

# I'm using the equivalent p/s codes to indicate unevaluated bytes vs actual
# ascii / "alpha" data

# Using ord here to make lookups more straightforward with bytes (not sure why
# rec[0] below doesn't return a byte...)
std_prefix = '>c2h6p'
rec_types = {ord('S'): std_prefix + 'c',              # System Event Message
             ord('R'): std_prefix + '8s2ci2c2s5cic',  # Stock Directory Message
             }

with gzip.GzipFile(sys.argv[1]) as infile:
    while(True):
        rec_len, = struct.unpack('>h', infile.read(2))
        # print('rec_len:', rec_len)
        rec = infile.read(rec_len)
        try:
            # print('rec_type:', rec[0])
            fmt = rec_types[rec[0]]
            # print(fmt)
            unpacked_rec = struct.unpack(fmt, rec)
            print(unpacked_rec)
        except KeyError:
            # Silently ignore unknown record types
            # (at least until we have them all)
            # print('Unkown!')
            pass
