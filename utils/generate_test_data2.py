'''Mix up symbols and prices a little bit'''

from zipfile import ZipFile
from os import path

import taq
import taq.processing as tp

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('fname_in', help="Path to Zipped TAQ data")
parser.add_argument('fname_out', default='test_data_public',
                    help="Path to write output"
                         "(both zip archive and contained file")
parser.add_argument('size', type=int,
                    help="Integer number of lines to sanitize and write")
parser.add_argument('--frac', '-f', type=float, default=0.001,
                    help='Floating point probability of returning each line')
args = parser.parse_args()

taq_in = taq.TAQ2Chunks(args.fname_in, do_process_chunk=False)
downsampled = tp.downsample(taq_in, args.frac)
sanitized = tp.Sanitizer(tp.split_chunks(downsampled, 'Symbol_root'))

with open(args.fname_out, 'wb') as ofile:
    writ_len = 0
    ofile.write(taq_in.first_line)

    for chunk in sanitized:
        if len(chunk) + writ_len > args.size:
            break
        ofile.write(chunk)
        writ_len += len(chunk)

basename = path.basename(args.fname_out)
with ZipFile(args.fname_out + '.zip', 'w') as zf:
    zf.write(args.fname_out, basename)

# Currently, the unzipped version of args.fname_out is left laying around!
