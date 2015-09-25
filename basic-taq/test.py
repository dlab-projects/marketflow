#!/usr/bin/env python3

import raw_taq
import pandas as pd
import numpy as np
from statistics import mode, StatisticsError

def process_chunks(taq):
    chunk_gen = taq.convert_taq(20)     #create a generator for calling each chunk
    first_chunk = next(chunk_gen)

    accum = pd.DataFrame(first_chunk)

    for chunk in chunk_gen:
        accum.append(pd.DataFrame(chunk))
    print(accum)

if __name__ == '__main__':
    # fname = '../local_data/EQY_US_ALL_BBO_20150102.zip'
    # fname = '../local_data/EQY_US_ALL_BBO_20140206.zip'
    from sys import argv
    fname = '../local_data/EQY_US_ALL_BBO_201402' + argv[1] + '.zip'
    print("processing", fname)
    
    local_taq = raw_taq.TAQ2Chunks(fname)
    process_chunks(local_taq)
