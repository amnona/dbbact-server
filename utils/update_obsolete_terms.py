#!/usr/bin/env python

''' Update the AnnotationParentsTable and the corresponding sequence counts
use it after adding/updating/changing an ontology in dbbbact
'''

import sys

import argparse
import setproctitle
import re
from collections import defaultdict

import oboparse
import psycopg2

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.1"


def update_obsolete_terms(con, cur, ontofilename, ontology_name=None, commit=True):
	'''replace obsolete terms as indicated by "replaved_by" in the new ontology.
	This is done by updating the annotations - the old term is replaced by the new term
	NOTE: this is done only if the term only participates in a single ontology (in the tree structure)

	Parameters
	----------
	con, cur: dbbact psycopg2 database connection and cursor
	ontofilename : str
		name of the .obo ontology file to add
	ontology_name : str or None, optional
		if not None, update only terms that appear only in this ontology tree (i.e. 'silva')
	'''
	# we need 2 phases since in dbbact we store the name, whereas the replaced_by stores the id
	if ontology_name is not None:
		cur.execute('SELECT id FROM OntologyNamesTable WHERE description=%s LIMIT 1', [ontology_name])
		if cur.rowcount == 0:
			raise ValueError('ontology %s not found in OntologyNamesTable. stopping')
		ontology_name_id = cur.fetchone()[0]
	else:
		ontology_name_id = None
	debug(3, 'phase 1: getting obsolete terms')
	# phase1 - get the required ids
	parser = oboparse.Parser(open(ontofilename))
	ids_to_get = defaultdict(list)
	num_obsolete = 0
	num_to_replace = 0
	for citem in parser:
		tags = citem.tags
		cid = tags["id"][0]
		# just obsolete terms
		if "is_obsolete" not in tags:
			continue
		if tags["is_obsolete"][0].lower() != 'true':
				continue
		num_obsolete += 1
		# and we need the replaced_by field
		if "replaced_by" not in tags:
			continue
		replaced_id = tags['replaced_by'][0].lower()
		if replaced_id == 'false':
			continue
		if "name" not in tags:
			continue
		orig_name = tags['name'][0].lower()
		orig_name = re.sub('obsolete ', '', orig_name, 1)
		ids_to_get[replaced_id].append(orig_name)
		num_to_replace += 1
	debug(3, 'found %d obsolete terms. %d to replace, with %d new term ids' % (num_obsolete, num_to_replace, len(ids_to_get)))

	debug(3, 'phase2: replacing original terms in annotations')
	# phase2: go over all terms, and if in list, replace these new values instead of the old ones
	parser = oboparse.Parser(open(ontofilename))
	for citem in parser:
		tags = citem.tags
		cid = tags["id"][0].lower()
		if cid not in ids_to_get:
			continue
		if 'name' not in tags:
			debug(4, 'need to replace with term %s but no name supplied' % cid)
			continue
		cname = tags['name'][0]
		cur.execute('SELECT id FROM OntologyTable WHERE description=%s LIMIT 1', [cname])
		if cur.rowcount == 0:
			debug(6, 'new term %s not found in ontology table' % cname)
			continue
		contoid = cur.fetchone()[0]
		for cobsolete_term in ids_to_get[cid]:
			cur.execute('SELECT id FROM OntologyTable WHERE description=%s LIMIT 1', [cobsolete_term])
			if cur.rowcount == 0:
				debug(6, 'obsolete term %s for new term %s not found in ontology table' % (cobsolete_term, cname))
				continue
			cobsolete_id = cur.fetchone()[0]
			if ontology_name_id is not None:
				# make sure the obsolete term does not participate in other ontologies
				cur.execute('SELECT ontologynameid FROM OntologyTreeStructureTable WHERE ontologyid=%s AND ontologynameid!=%s', [cobsolete_id, ontology_name_id])
				if cur.rowcount > 0:
					debug(6, 'obsolete term %s participates in other ontologies. skipping' % cobsolete_term)
					continue
				cur.execute('UPDATE OntologyTable SET replaced_by=%s WHERE id=%s', [contoid, cobsolete_id])

			debug(3, 'for term %s (%d) replace with term %s (%d)' % (cobsolete_term, cobsolete_id, cname, contoid))
			cur.execute('SELECT idannotation, idannotationdetail FROM AnnotationListTable WHERE idontology=%s', [cobsolete_id])
			debug(3, 'got %d annotations with this term' % cur.rowcount)
			res = cur.fetchall()
			for cres in res:
				cidannotation = cres['idannotation']
				cidannotationdetail = cres['idannotationdetail']
				cur.execute('SELECT * FROM AnnotationListTable WHERE idannotation=%s AND idannotationdetail=%s AND idontology=%s', [cidannotation, cidannotationdetail, contoid])
				if cur.rowcount == 0:
					cur.execute('INSERT INTO AnnotationListTable (idannotation, idannotationdetail, idontology) VALUES (%s, %s, %s)', [cidannotation, cidannotationdetail, contoid])
				else:
					debug(5, 'entry already exists for annotation %d' % cidannotation)
				cur.execute('DELETE FROM AnnotationListTable WHERE idannotation=%s AND idannotationdetail=%s AND idontology=%s', [cidannotation, cidannotationdetail, cobsolete_id])
			debug(3, 'did it for term %s replace with term %s' % (cobsolete_term, cname))
	if commit:
		con.commit()
	debug(3, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='Replace obsoloete ontology terms after running add_ontology. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('-i', '--input', help='ontology file name (.obo) to update with')
	parser.add_argument('--ontology', help='ontology database name (i.e. "envo"). if provided, only update terms not appearing in other ontologies', default=None)
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
	update_obsolete_terms(con, cur, ontofilename=args.input, ontology_name=args.ontology)


if __name__ == "__main__":
	main(sys.argv[1:])
