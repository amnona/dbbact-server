#!/usr/bin/env python

"""
dbbact maintenance script for adding OBO ontologies to database
using the rest API
"""

# amnonscript

__version__ = "0.6"

import oboparse

import requests

import argparse
import sys


def getidname(ontofilename):
	"""
	create the id->name dict for the ontology in ontofilename

	input:
	ontofilename : str
		name of the ontology obo file

	output:
	idname : dict {str:str}
		dict with id as key, name as value
	"""
	idname = {}
	numtot = 0
	print('initializing idname from file %s' % ontofilename)
	parser = oboparse.Parser(open(ontofilename))
	for citem in parser:
		numtot += 1
		try:
			cid = citem.tags["id"][0]
			cname = citem.tags["name"][0]
			if cid in idname:
				print("id %s already exists!" % cid)
			idname[cid] = cname
		except:
			continue
	print('loaded %d ids out of %d entries' % (len(idname), numtot))
	return idname


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
	parser = argparse.ArgumentParser(description='Add ontology file to database. Version ' + __version__)
	parser.add_argument('-i', '--input', help='ontology file name (.obo)')
	parser.add_argument('-n', '--name', help='name for the ontolgy (i.e ENVO)')
	parser.add_argument('-s', '--server', help='web address of the server', default='http://127.0.0.1:7001')
	args = parser.parse_args(argv)
	addontology(ontofilename=args.input, ontoname=args.name, dbserver=args.server)


if __name__ == "__main__":
	main(sys.argv[1:])
