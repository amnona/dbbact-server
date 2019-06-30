#!/usr/bin/env python

# amnonscript

import argparse
from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access
from dbbact_server.dbannotations import DeleteAnnotation

import sys

__version__ = "0.1"


def delete_annotation(con, cur, annotationid, userid=0, delete=False, commit=False):
	debug(3, 'delete annotation %d' % annotationid)
	if delete:
		res = DeleteAnnotation(con, cur, annotationid=annotationid, userid=userid, commit=False)
		if res:
			debug(5, res)
	if commit:
		con.commit()


def main(argv):
	parser = argparse.ArgumentParser(description='delete_annotations version %s\ndelete sequences not in any annotation' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--annotationids', help='list of annotation ids to delete (space separated)', nargs='+', type=int)
	parser.add_argument('--expids', help='list of experiment ids to delete (space separated)', nargs='+', type=int)
	parser.add_argument('--delete', help='delete the sequences', action='store_true')
	parser.add_argument('--log-level', help='output level (1 verbose, 10 error)', type=int, default=3)

	args = parser.parse_args(argv)
	SetDebugLevel(args.log_level)
	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)

	annotationids = []
	# fill the annotations from each experiment
	if args.expids:
		for cexpid in args.expids:
			cur.execute("SELECT id from AnnotationsTable WHERE idexp=%s", [cexpid])
			for cres in cur:
				annotationids.append(cres[0])
		debug(3, 'found %d annotations for the experiments' % len(annotationids))
	# and add the annotation ids supplied
	if args.annotationids is not None:
		annotationids.extend(args.annotationids)

	for cannotationid in annotationids:
		# get the user that created the annotation
		cur.execute("SELECT iduser FROM AnnotationsTable WHERE id=%s LIMIT 1", [cannotationid])
		res = cur.fetchone()
		cuserid = res['iduser']
		# and delete
		delete_annotation(con, cur, annotationid=cannotationid, userid=cuserid, delete=args.delete)

	debug(3, 'committing')
	con.commit()
	debug(3, 'done. please run delete_unused_seqs.py to remove unused sequences')


if __name__ == "__main__":
	main(sys.argv[1:])
