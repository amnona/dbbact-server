#!/usr/bin/env python

# amnonscript

import argparse
from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access
import sys

__version__ = "1.0"


def add_primer_to_annotations(con, cur, update_all=False, commit=True):
	'''Update the primerID field in the AnnotationsTable according to the sequences in the annotation

	Parameters
	----------
	update_all: bool, optional
		if True, update all annotations. If False, update only annotations with 'na' (primerID=0) in the primerId field)
	commit: bool, optional
		True to commit changes to database
	'''
	if update_all:
		cur.execute('SELECT id FROM AnnotationsTable')
	else:
		cur.execute('SELECT id FROM AnnotationsTable WHERE PrimerID=0')
	res = cur.fetchall()
	idx = 0
	for idx, cres in enumerate(res):
		cid = cres['id']
		cur.execute('SELECT seqID from SequencesAnnotationTable WHERE annotationID=%s', [cid])
		res2 = cur.fetchall()
		cprimerid = None
		for cres2 in res2:
			cseqid = cres2['seqid']
			cur.execute('SELECT idPrimer from SequencesTable WHERE id=%s LIMIT 1', [cseqid])
			res3 = cur.fetchone()
			if cprimerid is None:
				cprimerid = res3['idprimer']
			if res3['idprimer'] != cprimerid:
				debug(8, 'annotation %d contains sequences from two different regions' % cid)
				cprimerid = None
				break
		if cprimerid is None:
			debug(7, "didn't find primer region for annotation %d. skipping" % cid)
			continue
		debug(2, 'annotation %d primer region %d' % (cid, cprimerid))
		cur.execute('UPDATE AnnotationsTable SET primerID=%s WHERE id=%s', [cprimerid, cid])
	debug(3, 'found %d annotations' % idx)
	if commit:
		debug(3, 'committing changes to database')
		con.commit()
	debug(3, 'finished')


def main(argv):
	parser = argparse.ArgumentParser(description='add_primer_to_annotations.py version %s\nupdate the primer region on the dbbact annotations' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--log-level', help='output level (1 verbose, 10 error)', type=int, default=3)
	parser.add_argument('--all', help='update all annotations (if not specified, update only annotations with na in primer)', action='store_true')

	args = parser.parse_args(argv)
	SetDebugLevel(args.log_level)
	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	add_primer_to_annotations(con, cur, update_all=args.all)


if __name__ == "__main__":
	main(sys.argv[1:])
