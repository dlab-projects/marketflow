from glob import glob
from itertools import (islice, zip_longest)
from collections import Counter
import numpy as np
from sys import argv

import raw_taq

def count_chunk_elements(fname, chunksize = 1000000, max_chunk = None, process_chunk = True):

    symbol_roots = Counter()

    for (i,chunk) in enumerate(islice(raw_taq.TAQ2Chunks(fname, chunksize=chunksize, do_process_chunk=process_chunk), max_chunk)):

        counts = np.unique(chunk[:]['Symbol_root'], return_counts=True)
        symbol_roots.update(dict(zip_longest(counts[0], counts[1])))

        #print("\r {0}".format(i),end="")

    return symbol_roots

if __name__ == '__main__':
    fname = '../local_data/EQY_US_ALL_BBO_20150102.zip'

    c = count_chunk_elements(fname, chunksize = int(argv[1]), max_chunk=None, process_chunk = False)
    for (i,(k,v)) in enumerate(islice(c.most_common(),100)):
        print ("\t".join([str(i), k.decode('utf-8').strip(), str(v)]))
