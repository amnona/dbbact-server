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


def get_filter_file(filter_file_name):
	'''Load the filter ids file (used for keeping only ontology terms from this file - for ncbitaxonomy)

	Parameters
	----------
	filter_file_name: str or None
		name of the filter ids file (one id per line, without the 'NCBITaxon:')
		get it from pubmed - list of sub taxonomy and save to file


	Returns
	-------
	filter_ids: set
		ids to keep. if filter_file_name is None, return None
	'''
	filter_ids = set()
	if filter_file_name is None:
		return None
	print('loading ids for filtering from file %s' % filter_file_name)
	with open(filter_file_name) as fl:
		for cline in fl:
			filter_ids.add(cline.strip())
	print('loaded %d ids for filtering' % len(filter_ids))
	return filter_ids


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
			# remove ' {' from names
			if cname is not None:
				if ' {' in cname:
					cname = cname.split(' {')[0]
			if cid in idname:
				print("id %s already exists!" % cid)
			idname[cid] = cname
		except:
			continue
	print('loaded %d ids out of %d entries' % (len(idname), numtot))
	return idname


def addontology(ontofilename, ontoname, dbserver='http://127.0.0.1:7001', ontoprefix='', filter_file_name=None):
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
	filter_file_name: str or None, optional
		if None, keep all ids.
		if not None: name of the filter ids file (one id per line, without the 'NCBITaxon:')
		get it from pubmed - list of sub taxonomy and save to file
	"""

	url = dbserver + '/ontology/add'
	idname = getidname(ontofilename)
	filter_ids = get_filter_file(filter_file_name)
	parser = oboparse.Parser(open(ontofilename))

	num_non_term = 0
	for citem in parser:
		if citem.name.lower() != 'term':
			num_non_term += 1
			continue

		tags = citem.tags
		cid = tags["id"][0]

		# if filtering, test if we should handle this id
		if filter_ids:
			if cid.split(':')[-1] not in filter_ids:
				continue

		# if no name supplied for ontology, take it from the first entry
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
			# remove ' {' from names
			if ' {' in origname:
				origname = origname.split(' {')[0]
		else:
			print("ontology item id %s does not have a name" % cid)
			origname = ''

		# get the synonyms
		if "synonym" in tags:
			synonyms = tags["synonym"]
		else:
			synonyms = None

		# add the initial term entry with synonyms
		data = {'term': origname, 'synonyms': synonyms, 'parent': None, 'ontologyname': ontoname, 'term_id': cid, 'parent_id': None}
		res = requests.post(url, json=data)

		# get the parents
		parent_ids = []
		if "is_a" in tags:
			for parentid in tags['is_a']:
				# see if we have a comment ('{' before the '!')
				if parentid is not None:
					if parentid not in idname:
						if ' {' in parentid.split('!')[0]:
							print('found comment (" {") for term %s' % parentid)
							parentid = parentid.split(' {')[0]
				parent_ids.append(parentid)
		if "relationship" in tags:
			for rela in tags['relationship']:
				rela = rela.split(' ', 1)
				if rela[0] in ['derives_from', 'located_in', 'part_of', 'develops_from', 'participates_in']:
					parentid = rela[1]
					if ' {' in parentid.split('!')[0]:
						print('found comment (" {") for term %s' % parentid)
						parentid = parentid.split(' {')[0]
					parent_ids.append(parentid)

		# add all parents
		for cparent_id in parent_ids:
			# get the parent name
			if cparent_id in idname:
				cparent_name = idname[cparent_id]
			else:
				print("parentid %s not found" % cparent_id)
				cparent_name = 'NA'
			data = {'term': origname, 'synonyms': synonyms, 'parent': cparent_name, 'ontologyname': ontoname, 'term_id': cid, 'parent_id': cparent_id}
			res = requests.post(url, json=data)
	print('skipped %d non-terms' % num_non_term)
	print('done')


def main(argv):
	parser = argparse.ArgumentParser(description='Add ontology file to database. Version ' + __version__)
	parser.add_argument('-i', '--input', help='ontology file name (.obo)')
	parser.add_argument('-n', '--name', help='name for the ontolgy (i.e ENVO)')
	parser.add_argument('-s', '--server', help='web address of the server', default='http://127.0.0.1:7001')
	parser.add_argument('--filter', help='filter file name. if provided, add only terms with ids present in the filter file')
	args = parser.parse_args(argv)
	addontology(ontofilename=args.input, ontoname=args.name, dbserver=args.server, filter_file_name=args.filter)


if __name__ == "__main__":
	main(sys.argv[1:])
