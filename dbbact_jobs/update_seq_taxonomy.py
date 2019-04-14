#!/usr/bin/env python

import sys
import argparse
import os

from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access


__version__ = '0.9'


def update_seq_taxonomy(con, cur, rdp='rdp_classifier_2.12/'):
    debug(3, 'Started update_seq_taxonomy')

    # create the fasta file with only sequences with no taxonomy
    num_missing_tax = 0
    fasta_file_name = 'sequences-no-taxonomy.fa'
    output_file_name = 'sequences-tax.rdp.txt'
    with open(fasta_file_name, 'w') as fl:
        num_missing_tax += 1
        cur.execute("SELECT sequence FROM SequencesTable WHERE COALESCE(taxonomy, 'na') = 'na' ")
        for cres in cur:
            fl.write('>%s\n%s\n' % (cres[0], cres[0]))
    debug(2, 'found %d sequences with missing taxonomy. saved to fasta file %s' % (num_missing_tax, fasta_file_name))
    # run RDP
    os.system("java -Xmx1g -jar %sdist/classifier.jar classify  -o %s %s" % (rdp, output_file_name, fasta_file_name))
    debug(2, 'RDP finished. results in %s' % output_file_name)

    ok_levels = ['taxrootrank', 'taxdomain', 'taxphylum', 'taxclass', 'taxorder', 'taxfamily', 'taxgenus']
    ok_levels_set = set(ok_levels)
    with open(output_file_name) as fl:
        for cline in output_file_name:
            ctaxonomy = [''] * len(ok_levels)
            cdat = cline.split('\t')
            cseq = cdat.pop(0)
            while len(cdat) > 0:
                cval = cdat.pop(0)
                clevel = cdat.pop(0).lower()
                cdblevel = 'tax' + clevel
                if cdblevel not in ok_levels_set:
                    debug(1, 'level %s not in ok levels' % cdblevel)
                    continue
                cprob = cdat.pop(0)
                if cprob < 0.8:
                    continue
                cur.execute('UPDATE SequencesTable SET %s=%s WHERE sequence=%s', [cdblevel, cval, cseq])
                ctaxonomy[ok_levels.index(cdblevel)] = cval
            # generate the full taxonomy string
            max_non_empty = -1
            for idx in range(len(ctaxonomy)):
                if ctaxonomy[idx] != '':
                    max_non_empty = idx
            if max_non_empty == -1:
                taxstr = 'unknown'
            else:
                taxstr = ';'.join(ctaxonomy[:max_non_empty + 1]) + ';'
            cur.execute('UPDATE SequencesTable SET taxonomy=%s WHERE sequence=%s', [taxstr, cseq])
    debug(2, 'finished updating database. committing')
    con.commit()


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
