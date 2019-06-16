#!/usr/bin/env python

# Update the total number of annotations and experiments each term appears in

'''Update the total number of annotations and experiments each term appears in
'''

import sys

import argparse
import psycopg2
import psycopg2.extras
import setproctitle
from collections import defaultdict

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.9"


def update_term_info(con, cur):
	cur2 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
	debug(3, 'update_term_info started')
	debug(2, 'dropping old TermInfoTable')
	cur.execute('DELETE FROM TermInfoTable')
	debug(2, 'processing annotations')
	term_pos_exps = defaultdict(set)
	term_neg_exps = defaultdict(set)
	term_pos_anno = defaultdict(set)
	term_neg_anno = defaultdict(set)
	all_term_ids = set()
	# iterate all annotationes / annotationsdetails
	cur.execute('SELECT id, idexp FROM AnnotationsTable')
	for idx, cres in enumerate(cur):
		cannoid = cres['id']
		cexp = cres['idexp']
		if idx % 1000 == 0:
			debug(2, 'processing annotation %d' % cannoid)
		cur2.execute('SELECT idontology, idannotationdetail FROM AnnotationListTable WHERE idannotation=%s', [cannoid])
		for cdres in cur2:
			ctype = cdres['idannotationdetail']
			ctermid = cdres['idontology']
			all_term_ids.add(ctermid)
			# if LOWER IN
			if ctype == 2:
				term_neg_exps[ctermid].add(cexp)
				term_neg_anno[ctermid].add(cannoid)
			else:
				term_pos_exps[ctermid].add(cexp)
				term_pos_anno[ctermid].add(cannoid)

	debug(3, 'Found %d terms' % len(all_term_ids))
	debug(2, 'adding stats to TermInfoTable')
	for ctermid in all_term_ids:
		cur2.execute('SELECT description FROM OntologyTable WHERE id=%s LIMIT 1', [ctermid])
		if cur2.rowcount == 0:
			debug(5, 'no term name in OntologyTable for termid %d. skipping' % ctermid)
			continue
		res = cur2.fetchone()
		cterm = res[0]
		if ctermid in term_pos_exps:
			cur2.execute('INSERT INTO TermInfoTable (term, TotalExperiments, TotalAnnotations,TermType) VALUES (%s, %s, %s, %s)', [cterm, len(term_pos_exps[ctermid]), len(term_pos_anno[ctermid]), 'single'])
		if ctermid in term_neg_exps:
			cur2.execute('INSERT INTO TermInfoTable (term, TotalExperiments, TotalAnnotations,TermType) VALUES (%s, %s, %s, %s)', ['-' + cterm, len(term_neg_exps[ctermid]), len(term_neg_anno[ctermid]), 'single'])

	debug(2, 'committing')
	con.commit()
	debug(3, 'done')


def update_term_info_old(con, cur):
	cur2 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur3 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

	debug(3, 'update_term_info started')
	debug(2, 'dropping old TermInfoTable')
	cur.execute('DELETE FROM TermInfoTable')
	debug(2, 'processing terms')
	cur.execute('SELECT id,description FROM OntologyTable')
	for idx, cres in enumerate(cur):
		term_exps_pos = set()
		term_exps_neg = set()
		term_annotations_pos = set()
		term_annotations_neg = set()
		ctermid = cres['id']
		cterm = cres['description']
		# get all the annotations containing this term
		cur2.execute('SELECT idannotation,idannotationdetail FROM AnnotationListTable WHERE idontology=%s', [ctermid])
		for ctres in cur2:
			ctype = ctres['idannotationdetail']
			cannotation = ctres['idannotation']

			# get more info about the annotation
			cur3.execute('SELECT idexp FROM AnnotationsTable WHERE id=%s LIMIT 1', [cannotation])
			cares = cur3.fetchone()
			cexp = cares['idexp']

			# if it's "LOWER IN cterm" it is neg
			if ctype == 2:
				term_exps_neg.add(cexp)
				term_annotations_neg.add(cannotation)
			else:
				term_exps_pos.add(cexp)
				term_annotations_pos.add(cannotation)

		cur2.execute('INSERT INTO TermInfoTable (term, TotalExperiments, TotalAnnotations,TermType) VALUES (%s, %s, %s, %s)', [cterm, len(term_exps_pos), len(term_annotations_pos), 'single'])
		cur2.execute('INSERT INTO TermInfoTable (term, TotalExperiments, TotalAnnotations,TermType) VALUES (%s, %s, %s, %s)', ['-' + cterm, len(term_exps_neg), len(term_annotations_neg), 'single'])
		if idx % 1000 == 0:
			debug(2, 'processed term %d: %s. pos exps %d, pos anno %d, neg exps %d, neg anno %d' % (idx, cterm, len(term_exps_pos), len(term_annotations_pos), len(term_exps_neg), len(term_annotations_neg)))
		if cterm == 'small village':
			debug(2, 'processed term %d: %s. pos exps %d, pos anno %d, neg exps %d, neg anno %d' % (idx, cterm, len(term_exps_pos), len(term_annotations_pos), len(term_exps_neg), len(term_annotations_neg)))

	debug(2, 'committing')
	con.commit()
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Add annotation/experiment counts to all dbbact sequences. version ' + __version__)
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
	update_term_info(con, cur)


if __name__ == "__main__":
	main(sys.argv[1:])
