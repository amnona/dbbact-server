#!/usr/bin/env python

''' Delete unused terms in dbbact ontology table
Only removed terms not in annotations or ontologytreestructure
'''

import sys

import argparse
import setproctitle

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.1"


def delete_unused_terms(con, cur, commit=True):
	'''Delete all unused terms from OntologyTable
	only delete terms that are not in annotations or tree structure

	Parameters
	----------
	con, cur
	commit: bool, optional
	True to commit the changes to the database. False to run without changing
	'''
	debug(3, 'deleting unused terms')
	num_deleted = 0
	cur.execute('SELECT id, description FROM OntologyTable')
	res = cur.fetchall()
	debug(3, 'found %d terms' % len(res))
	for cres in res:
		cid = cres['id']
		cterm = cres['description']
		# do we use it in an annotation?
		cur.execute('SELECT * FROM AnnotationListTable WHERE idontology=%s', [cid])
		if cur.rowcount > 0:
			continue
		# is it in the ontology tree as child?
		cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyid=%s', [cid])
		if cur.rowcount > 0:
			continue
		# or as parent?
		cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyparentid=%s', [cid])
		if cur.rowcount > 0:
			continue
		# ok so not used, let's delete it
		# first delete from synonymstable
		cur.execute('DELETE FROM OntologySynonymTable WHERE idontology=%s', [cid])
		cur.execute('DELETE FROM OntologyTable WHERE id=%s', [cid])
		num_deleted += 1
	debug(3, 'found %d unused terms to delete' % num_deleted)
	if commit:
		con.commit()
		debug(3, 'committed')
	else:
		debug(4, 'not committing changes. nothing was deleted')
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Delete dbbact terms not used in annotations or tree structure. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--proc-title', help='name of the process (to view in ps aux)')
	parser.add_argument('--delete', help='MUST provide this in order to actually delete', action='store_true')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)
	# set the process name for ps aux
	if args.proc_title:
		setproctitle.setproctitle(args.proc_title)

	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	delete_unused_terms(con, cur, commit=args.delete)


if __name__ == "__main__":
	main(sys.argv[1:])
