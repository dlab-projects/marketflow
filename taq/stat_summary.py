#!/usr/bin/env python3

import raw_taq
import pandas as pd
import numpy as np
from statistics import mode, StatisticsError

def print_stats(chunk):              #print the stat for bid price
    bid_price = chunk['Bid_Price']

    max_price = max(bid_price)
    min_price = min(bid_price)
    avg_price = np.mean(bid_price)
    sd_price = np.std(bid_price)
    try:
        mod_price = mode(bid_price)
    except StatisticsError:
        mod_price = np.nan

    print("Max bid price: ", max_price, "\n", "Min bid price: ", min_price, "\n",
          "Mean bid price: ", avg_price, "\n", "Mod bid price: ", mod_price, "\n",
          "Standard deviation bid price: ", sd_price)

def process_chunks(taq):
    chunk_gen = taq.convert_taq(20)     #create a generator for calling each chunk
    first_chunk = next(chunk_gen)
    curr_symbol = first_chunk['Symbol_root'][0]

    accum = pd.DataFrame(first_chunk)

    processed_symbols = 0
    row = 0
    for chunk in chunk_gen:     
        where_symbol = curr_symbol == chunk['Symbol_root']      #logistical return for checking symbol_root
        if where_symbol.all():
            accum.append(pd.DataFrame(chunk))
            row = row + 20
        else:
            same = chunk[where_symbol]
            accum.append(pd.DataFrame(same))
            row = row + sum(where_symbol)

            print('Current symbol:', curr_symbol, len(curr_symbol), 'records ', row, 'rows')
            print_stats(accum)
            processed_symbols += 1
            if processed_symbols > 2:
                break

            diff = chunk[~where_symbol]
            row = sum(~where_symbol)
            accum = pd.DataFrame(diff)
            curr_symbol = accum.Symbol_root[0]

if __name__ == '__main__':
    from sys import argv
    fname = '../../local_data/EQY_US_ALL_BBO_201501' + argv[1] + '.zip'
    #print("processing", fname)
    local_taq = raw_taq.TAQ2Chunks(fname)
    process_chunks(local_taq)
