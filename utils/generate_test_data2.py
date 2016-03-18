'''Mix up symbols and prices a little bit'''

from zipfile import ZipFile

import taq
import taq.processing as tp

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

    taq_in = taq.TAQ2Chunks(args.fname_in, do_process_chunk=False)
    sanitized = tp.Sanitizer(tp.split_chunks(taq_in, 'Symbol_root'))

    with open(args.fname_out, 'wb') as ofile:
        writ_len = 0
        ofile.write(taq_in.first_line)

        for chunk in sanitized:
            if len(chunk) + writ_len > args.size:
                break
            ofile.write(chunk)
            writ_len += len(chunk)

    with ZipFile(args.fname_out + '.zip', 'w') as zf:
        zf.write(args.fname_out)

    # Currently, the unzipped version of args.fname_out is left laying around!
