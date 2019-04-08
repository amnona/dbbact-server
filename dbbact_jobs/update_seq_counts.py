#!/usr/bin/env python

# Add the total counts of annotations and experiments for each sequence in dbbact

'''Add the total counts of annotations and experiments for each sequence in dbbact
'''

import sys
from collections import defaultdict

import argparse
import psycopg2
import psycopg2.extras

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.9"


def add_seq_counts(con, cur):
	cur2 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

	seq_exps = defaultdict(set)
	seq_annotations = defaultdict(set)

	debug(3, 'add_seq_counts started')
	debug(2, 'processing sequences')
	cur.execute('SELECT seqid,annotationid FROM SequencesAnnotationTable')
	for cres in cur:
		cseq_id = cres['seqid']
		canno_id = cres['annotationid']
		cur2.execute('SELECT idexp FROM AnnotationsTable WHERE id=%s LIMIT 1', [canno_id])
		cres2 = cur2.fetchone()
		if cur2.rowcount != 0:
			cexp_id = cres2[0]
			seq_exps[cseq_id].add(cexp_id)
			if canno_id in seq_annotations[cseq_id]:
				debug(5, 'sequence %s already associated with annotation %s' % (cseq_id, canno_id))
			seq_annotations[cseq_id].add(canno_id)
		else:
			debug(5, 'sequence %s annotationid %s does not exist in annotationstable' % (cseq_id, canno_id))

	debug(2, 'found data for %d sequences' % len(seq_exps))
	debug(2, 'adding total_annotations, total_experiments to SequencesTable')
	for cseq_id in seq_annotations.keys():
		cur.execute('UPDATE SequencesTable SET total_annotations=%s, total_experiments=%s WHERE id=%s', [len(seq_annotations[cseq_id]), len(seq_exps[cseq_id]), cseq_id])
	con.commit()
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Add annotation/experiment counts to all dbbact sequences. version ' + __version__)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)
	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	add_seq_counts(con, cur)


if __name__ == "__main__":
	main(sys.argv[1:])
