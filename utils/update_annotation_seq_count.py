#!/usr/bin/env python

# Update the number of sequences in each annotation
# used after fixing the annotation seqcount update bug in delete sequence from annotation

'''Add the number of sequences per annotation
'''

import sys

import argparse
import psycopg2
import psycopg2.extras

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.9"


def add_annotation_seq_count(con, cur):
	cur2 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

	debug(3, 'add_annotation_seq_count started')
	debug(2, 'processing annotations')
	# iterate over all annotations
	num_anno = 0
	cur.execute('SELECT id FROM AnnotationsTable')
	for cres in cur:
		cid = cres['id']
		cur2.execute('SELECT COUNT(*) FROM sequencesannotationtable WHERE annotationid=%s', [cid])
		cres2 = cur2.fetchone()
		num_seqs = cres2[0]
		cur2.execute('UPDATE annotationstable SET seqcount=%s WHERE id=%s', [num_seqs, cid])
		num_anno += 1
	debug(2, 'scanned %d annotations.' % num_anno)
	debug(2, 'committing')
	con.commit()
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Add the number of sequences per annotation. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)

	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	add_annotation_seq_count(con, cur)


if __name__ == "__main__":
	main(sys.argv[1:])
