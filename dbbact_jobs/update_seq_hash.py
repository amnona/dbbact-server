#!/usr/bin/env python

import sys
import argparse
import hashlib

import setproctitle

from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access
from dbbact_server import dbsequences


__version__ = '0.9'


def update_seq_hash(con, cur):
    debug(3, 'Started update_seq_hash')
    count_success = 0
    count_failure = 0
    count_seq_success = 0
    count_seq_failure = 0
    count = 0

    while True:
        err, seq_id = dbsequences.GetSequenceWithNoHashID(con, cur)
        if err:
            debug(5, 'error encountered when searching for sequences with no hash: %s' % err)
            raise ValueError(err)
        if seq_id == -1:
            debug(3, 'no sequences to process. exiting')
            break

        hash_seq_full = ''
        hash_seq_150 = ''
        hash_seq_100 = ''

        err, seq_str = dbsequences.GetSequenceStrByID(con, cur, seq_id)
        if err:
            debug(5, 'could not find sequence %s' % seq_id)
            break

        hash_seq_full = 'na'
        hash_seq_150 = 'na'
        hash_seq_100 = 'na'

        seq_str = seq_str.upper()

        if len(seq_str) > 0:
            hash_seq_full = hashlib.md5(seq_str.encode('utf-8')).hexdigest()
        if len(seq_str) >= 150:
            hash_seq_150 = hashlib.md5(seq_str[:150].encode('utf-8')).hexdigest()
        if len(seq_str) >= 100:
            hash_seq_100 = hashlib.md5(seq_str[:100].encode('utf-8')).hexdigest()

        has_failure = False
        if dbsequences.UpdateHash(con, cur, seq_id, hash_seq_full, hash_seq_150, hash_seq_100):
            count_seq_success = count_seq_success + 1
        else:
            debug(5, 'failed to update hash for sequence %s' % seq_id)
            count_seq_failure = count_seq_failure + 1
            has_failure = True

        if has_failure:
            count_failure = count_failure + 1
        else:
            count_success = count_success + 1
        count += 1
    debug(2, 'added sequence hashes for %d sequences. %d failures' % (count, count_failure))


def main(argv):
    parser = argparse.ArgumentParser(description='update_seq_hash version %s' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--port', help='postgres port', default=5432, type=int)
    parser.add_argument('--host', help='postgres host', default=None)
    parser.add_argument('--database', help='postgres database', default='dbbact')
    parser.add_argument('--user', help='postgres user', default='dbbact')
    parser.add_argument('--password', help='postgres password', default='magNiv')
    parser.add_argument('--proc-title', help='name of the process (to view in ps aux)')
    parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
    args = parser.parse_args(argv)

    SetDebugLevel(args.debug_level)
    # set the process name for ps aux
    if args.proc_title:
        setproctitle.setproctitle(args.proc_title)

    # get the database connection
    con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)

    update_seq_hash(con, cur)


if __name__ == "__main__":
    main(sys.argv[1:])
