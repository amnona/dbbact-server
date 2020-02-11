#!/usr/bin/env python

''' Delete unused terms in dbbact ontology table
Only removed terms not in annotations or ontologytreestructure
'''

import sys

import argparse
import os
import pickle
from collections import defaultdict

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.9"


def prepare_dbbact_calour_term_files(con, cur, outdir='./', include_synonyms=True):
	'''Prepare the 2 ontology term pickle files needed for dbbact_calour new annotation term autocomplete.

	Output is saved into 2 files:

	ontology.pickle:
		dict of {name(str): ontologyid(str)}
			name:
				contains the full term/sysnonim name + "(+"ONTOLOGY NAME+"original term + ")". This is the string displayed to the user
			ontologyid:
				contains a unique id for this term that appears in the data/ontologyfromid.pickle file (loaded to DBAnnotateSave._ontology_from_id).

	ontologyfromid.pickle:
		dict of {ontologyid(str): term(str)}
			ontologyid:
				contains a unique id for each of the terms (linked from data/ontologies.pickle or DBAnnotateSave._ontology_dict)
			term:
				the dbbact term name

		For example for the term "united states of america" we have in DBAnnotateSave._ontology_dict key "U.S.A. :GAZ(United States of America)" with value GAZ:00002459
		and in DBAnnotateSave._ontology_from_id we have key "GAZ:00002459" with value "United States of America"

		Parameters
		----------
		outdir: str, optional
			name of the output dir where to save the pickle files
		include_synonyms: bool, optional
			True to add also all entries from synonyms table
	'''
	cur2 = con.cursor()
	cur2.execute('PREPARE find_syn(int) AS SELECT synonym FROM OntologySynonymTable WHERE idontology=$1')
	cur.execute('SELECT id, description, term_id FROM OntologyTable')
	debug(4, 'found %d terms' % cur.rowcount)

	term_name_id = defaultdict(dict)
	term_id_term = defaultdict(dict)
	num_terms = 0
	while True:
		res = cur.fetchone()
		if res is None:
			break
		num_terms += 1
		if num_terms % 100000 == 0:
			print(res)
		term_names = [res['description']]

		main_term = res['description']

		# also get all the synonyms for the term if needed
		if include_synonyms:
			cur2.execute('EXECUTE find_syn(%s)', [res['id']])
			if cur2.rowcount > 0:
				for cres2 in cur2:
					term_names.append(cres2[0])

		for cterm in term_names:
			ontology_name = 'NA'
			if ':' in res['term_id']:
				ontology_name = res['term_id'].split(':')[0]
			# if a synonym, put the original term in the parenthesis
			if cterm != main_term:
				term_name_id[ontology_name]['%s (%s - %s)' % (cterm, main_term, res['term_id'])] = res['id']
			# not sysnonym, so no need to add the original term - just the ENVO:XXXXX etc.
			else:
				term_name_id[ontology_name]['%s (%s)' % (cterm, res['term_id'])] = res['id']
			term_id_term[ontology_name][res['id']] = res['description']

	# move small ontologies to 'other' ontology
	small_ontologies = []
	all_ontologies = list(term_id_term.keys())
	for contology in all_ontologies:
		if len(term_name_id[contology]) < 500:
			term_name_id['other'].update(term_name_id[contology])
			term_id_term['other'].update(term_id_term[contology])
			del term_name_id[contology]
			del term_id_term[contology]
			small_ontologies.append(contology)
	print('moved %d small ontologies into other ontology:\n%s' % (len(small_ontologies), small_ontologies))

	# and save
	for contology in term_id_term.keys():
		with open(os.path.join(outdir, contology + '.ontology.pickle'), 'wb') as ofl:
			pickle.dump(term_name_id[contology], ofl)
		with open(os.path.join(outdir, contology + '.ontology.ids.pickle'), 'wb') as ofl:
			pickle.dump(term_id_term[contology], ofl)


def main(argv):
	parser = argparse.ArgumentParser(description='Prepare dbBact-calour term pickle files for autocomplete. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact_develop')
	parser.add_argument('--user', help='postgres user', default='dbbact_develop')
	parser.add_argument('--password', help='postgres password', default='dbbact_develop')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=4, type=int)
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)

	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	prepare_dbbact_calour_term_files(con, cur)


if __name__ == "__main__":
	main(sys.argv[1:])
