#!/usr/bin/env python

"""
dbbact maintenance script for finding duplicate entries in ontology.
Used to solve incrorrect tree structures
"""

# amnonscript

__version__ = "0.1"

import oboparse

import requests

import argparse
import sys


def find_duplicates(ontofilename):
	'''find entries with duplicate names in ontologies

	Parameters
	----------
	ontofilename : str
		name of the .obo ontology file to test
	'''
	terms = {}
	parser = oboparse.Parser(open(ontofilename))
	for citem in parser:
		tags = citem.tags
		cid = tags["id"][0]
		if "name" not in tags:
			continue
		origname = tags["name"][0]
		if origname in terms:
			print('term %s id %s also id %s' % (origname, cid, terms[origname]))
			continue
		terms[origname] = cid


def addontology(ontofilename, ontoname, dbserver='http://127.0.0.1:7001', ontoprefix=''):
	"""
	add all terms from an ontology obo file to the database

	input:
	ontofilename : str
		name of the .obo ontology file to add
	ontoname : str
		name of the ontology (for the OntologyNamesTable)
	dbserver : str
		the address where the database server is located (i.e. 127.0.0.1:5000)
	ontoprefix : str
		the ontology prefix (i.e. ENVO) to show at end of each string, or '' for autodetect (default)
	"""

	url = dbserver + '/ontology/add'
	idname = getidname(ontofilename)
	parser = oboparse.Parser(open(ontofilename))
	for citem in parser:
		tags = citem.tags
		cid = tags["id"][0]
		if len(ontoprefix) == 0:
			tt = cid.split(':')
			if len(tt) > 1:
				ontoprefix = tt[0]
		# do no add obsolete terms
		if "is_obsolete" in tags:
			if tags["is_obsolete"][0].lower() == 'true':
				continue
		if "name" in tags:
			origname = tags["name"][0]
		else:
			print("ontology item id %s does not have a name" % cid)
			continue
		if "synonym" in tags:
			synonyms = tags["synonym"]
		else:
			synonyms = None
		parent = 'NA'
		parentid = None
		if "is_a" in tags:
			parentid = tags["is_a"][0]
		elif "relationship" in tags:
			rela = tags["relationship"][0]
			rela = rela.split(' ', 1)
			if rela[0] in ['derives_from', 'located_in', 'part_of', 'develops_from', 'participates_in']:
				parentid = rela[1]
		if parentid is not None:
			if parentid in idname:
				parent = idname[parentid]
			else:
				print("parentid %s not found" % parentid)
		data = {'term': origname, 'synonyms': synonyms, 'parent': parent, 'ontologyname': ontoname}
		res = requests.post(url, json=data)
	print('done')


def main(argv):
	parser = argparse.ArgumentParser(description='Find duplicate terms in ontology. Version ' + __version__)
	parser.add_argument('-i', '--input', help='ontology file name (.obo)')
	args = parser.parse_args(argv)
	find_duplicates(ontofilename=args.input)


if __name__ == "__main__":
	main(sys.argv[1:])
