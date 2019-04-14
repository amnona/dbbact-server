#!/usr/bin/env python

import sys
import argparse
import os

from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access
from dbbact_server import dbsequences


__version__ = '0.9'


def createSeqFile(file_name, seq_str):
    with open(file_name, "w") as text_file:
        text_file.write(">seq\n%s" % seq_str)


def readResultFromFile(file_name):
    ret = ''
    try:
        with open(file_name, "r") as myfile:
            ret = myfile.readlines()
    except:
        debug(3, 'error reading results')
    return ret


def update_seq_taxonomy(con, cur, rdp='rdp_classifier_2.12/'):
    debug(3, 'Started update_seq_taxonomy')
    count_success = 0
    count_failure = 0
    count_seq_success = 0
    count_seq_failure = 0
    count = 0
    rank_list = []
    rank_list.append("rootrank")
    rank_list.append("life")
    rank_list.append("domain")
    rank_list.append("kingdom")
    rank_list.append("phylum")
    rank_list.append("class")
    rank_list.append("order")
    rank_list.append("family")
    rank_list.append("genus")
    rank_list.append("species")

    count_success = 0
    count_failure = 0
    count_seq_success = 0
    count_seq_failure = 0
    count = 1
    tax_log = ""

    while True:
        err, seq_id = dbsequences.GetSequenceWithNoTaxonomyID(con, cur)
        if err:
            debug(5, err)
            raise ValueError(err)
        if seq_id == -1:
            debug(2, 'No more sequences without taxonomy detected')
            break

        debug(1, 'looking for sequence without taxonomy')
        err, seq_str = dbsequences.GetSequenceStrByID(con, cur, seq_id)
        if err:
            raise ValueError(5, 'error encountered when searching for sequence id %d' % seq_id)

        debug(1, 'processing sequence %d: %s' % (count, seq_str))

        # java -Xmx1g -jar dist/classifier.jar classify  -o output_filename example.fasta
        input_file_name = "%sinput" % rdp
        output_file_name = "%soutput" % rdp

        # get the taxononmy for specific sequence
        createSeqFile(input_file_name, seq_str)
        os.system("java -Xmx1g -jar %sdist/classifier.jar classify  -o %s %s" % (rdp, output_file_name, input_file_name))
        tex_res = readResultFromFile(output_file_name)

        if len(tex_res) == 0:
            debug(2, 'got empty output for sequence %s' % seq_str)
            continue
        tax_log += "the data:\n"
        for line in tex_res:
            data = line.split('\t')

        # search for the string
        has_failure = False
        size_of_list = len(data)
        list_index = 0
        while list_index < size_of_list:
            has_failure = False
            curr_val = data[list_index]
            curr_val = curr_val.replace("\"", "")
            curr_val = curr_val.replace("\n", "")
            for y in rank_list:
                if curr_val == y:
                    if list_index > 0 & list_index < (size_of_list - 1):
                        # keep the next and previous value
                        prev_val = data[list_index - 1]
                        next_val = data[list_index + 1]
                        # remove unnecesary characters
                        prev_val = prev_val.replace("\"", "")
                        prev_val = prev_val.replace("\n", "")
                        next_val = next_val.replace("\"", "")
                        next_val = next_val.replace("\n", "")
                        if (float(next_val) >= 0.9):
                            # Add to DB
                            if dbsequences.AddSequenceTax(con, cur, seq_id, "tax" + curr_val, prev_val):
                                count_seq_success = count_seq_success + 1
                            else:
                                count_seq_failure = count_seq_failure + 1
                                has_failure = True
            list_index = list_index + 1
        if has_failure:
            count_failure = count_failure + 1
        else:
            count_success = count_success + 1
        count = count + 1
    debug(2, 'added sequence taxonomy for %d sequences. %d failures' % (count, count_failure))


def main(argv):
    parser = argparse.ArgumentParser(description='update_seq_hash version %s' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--port', help='postgres port', default=5432, type=int)
    parser.add_argument('--host', help='postgres host', default=None)
    parser.add_argument('--database', help='postgres database', default='dbbact')
    parser.add_argument('--user', help='postgres user', default='dbbact')
    parser.add_argument('--password', help='postgres password', default='magNiv')
    parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)

    parser.add_argument('--rdp', help='location of the rdp binary dir', default='rdp_classifier_2.12/')
    args = parser.parse_args(argv)

    SetDebugLevel(args.debug_level)
    # get the database connection
    con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)

    update_seq_taxonomy(con, cur, rdp=args.rdp)


if __name__ == "__main__":
    main(sys.argv[1:])
