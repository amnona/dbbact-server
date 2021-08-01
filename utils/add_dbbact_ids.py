#!/usr/bin/env python

''' Add the approriate dbbact ontology id (term_id) to each dbbact term in OntologyTable
need to run once since we didn't dutomatically set when adding new term
'''

import sys

import argparse
import setproctitle
from collections import defaultdict

import psycopg2

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.1"


def add_dbbact_ids(con, cur):
	''' Add the approriate dbbact ontology id (term_id) to each dbbact term in OntologyTable
	need to run once since we didn't dutomatically set when adding new term
	'''
	debug(3, 'getting terms without ontology term_id')
	cur.execute("SELECT * FROM ontologytable WHERE term_id=''")
	res = cur.fetchall()
	debug(3, 'found %d terms' % len(res))
	for cres in res:
		cid = cres['id']
		new_id_ontology = 'dbbact:%s' % cid
		cur.execute('UPDATE ontologytable SET term_id=%s WHERE id=%s', [new_id_ontology, cid])
	debug(3, 'committing')
	con.commit()
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Update the dbBact ontolgy term_id in OntologyTable for all dbbact terms. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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

	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	add_dbbact_ids(con, cur)


if __name__ == "__main__":
	main(sys.argv[1:])
