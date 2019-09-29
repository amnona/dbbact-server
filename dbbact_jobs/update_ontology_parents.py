#!/usr/bin/env python

''' Update the AnnotationParentsTable and the corresponding sequence counts
use it after adding/updating/changing an ontology in dbbbact
'''

import sys

import argparse
import setproctitle

from dbbact_server import db_access, dbannotations
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.9"


def update_ontology_parents(con, cur, overwrite=True):
	'''
	Fill the database AnnotationParentsTable

	Parameters
	----------
	servertype : str (optional)
		database to connect to ('main' or 'develop' or 'local')

	overwrite : bool (optional)
		False (default) to not overwrite existing annotation parents, True to delete all
	'''
	skipped = 0
	added = 0
	all_parents_dict = {}
	if overwrite:
		debug(4, 'deleting old parent counts')
		# delete the current counts since we are updating all entries (and addparents adds 1 to the counts...)
		cur.execute('UPDATE OntologyTable SET seqCount=0, annotationCount=0')
		debug(4, 'deleting annotationparentstable')
		cur.execute('DELETE FROM AnnotationParentsTable')
	cur.execute('SELECT id,seqCount from AnnotationsTable')
	annotations = cur.fetchall()
	debug(4, 'updating AnnotationParentsTable for %d annotations' % len(annotations))
	for cres in annotations:
		cid = cres[0]
		# if cid != 2305:
		# 	continue
		cseqcount = cres[1]
		if cseqcount == 0:
			debug(5, 'WARNING: annotation %d has no sequences in AnnotationsTable' % cid)
		# if not in overwrite mode, don't add parents to entries already in the table
		if not overwrite:
			cur.execute('SELECT idAnnotation from AnnotationParentsTable WHERE idAnnotation=%s', [cid])
			if cur.rowcount > 0:
				skipped += 1
				continue
		err, annotationdetails = dbannotations.GetAnnotationDetails(con, cur, cid)
		if err:
			debug(6, 'error: %s' % err)
			continue
		debug(3, 'adding annotation %d (%d)' % (cid, added))
		dbannotations.AddAnnotationParents(con, cur, cid, annotationdetails, commit=False, numseqs=cseqcount, all_parents_dict=all_parents_dict)
		added += 1
	debug(4, 'committing')
	con.commit()
	debug(4, 'added %d, skipped %d' % (added, skipped))


def main(argv):
	parser = argparse.ArgumentParser(description='Update the AnnotationParentsTable after changing ontologies. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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
	update_ontology_parents(con, cur)


if __name__ == "__main__":
	main(sys.argv[1:])
