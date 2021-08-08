#!/usr/bin/env python

'''Update the OntologyTreeStructureTable to fix the old na root term (which was undefined as contained many optional NAs)
'''

import sys

import argparse
import re
from collections import defaultdict

import oboparse
import psycopg2

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.1"


def fix_na(con, cur, commit=False):
	'''Update the OntologyTreeStructureTable to fix the old na root term (which was undefined as contained many optional NAs)

	Parameters
	----------
	con, cur: dbbact psycopg2 database connection and cursor
	commit: bool, optional
		True to commit changes, False to just perform dry run
	'''
	# find the dbbact root term id "dbbact root" (id 1811274)
	cur.execute('SELECT * from OntologyTable WHERE description=%s', ['dbbact root'])
	res = cur.fetchone()
	if res['term_id'] != 'dbbact:1811274':
		raise ValueError('"dbbact root" term_id is %s instead of dbbact:1811274' % res['term_id'])
	root_id = res['id']

	cur.execute('SELECT * FROM OntologyTable WHERE term_id LIKE %s', ['dbbact:%'])
	debug(3, 'Found %d dbbact terms' % cur.rowcount)
	res = cur.fetchall()
	num_na_parents = 0
	for cres in res:
		cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyid=%s', [cres['id']])
		tres = cur.fetchall()
		for ctres in tres:
			cur.execute('SELECT * FROM OntologyTable WHERE id=%s LIMIT 1', [ctres['ontologyparentid']])
			if cur.rowcount == 0:
				continue
			ttres = cur.fetchone()
			if ttres['description'] == 'na':
				cur.execute('UPDATE OntologyTreeStructureTable SET ontologyparentid=%s WHERE uniqueid=%s', [root_id, ctres['uniqueid']])
				num_na_parents += 1
	debug(4, 'updating %d dbbact terms roots' % num_na_parents)
	if commit:
		con.commit()
		debug(3, 'commited')
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Update the OntologyTreeStructureTable to fix the old na root term (which was undefined as contained many optional NAs). version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
	parser.add_argument('--dry-run', help='do not commit', action='store_true')
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)

	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	fix_na(con, cur, commit=not args.dry_run)


if __name__ == "__main__":
	main(sys.argv[1:])
