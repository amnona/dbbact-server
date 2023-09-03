#!/usr/bin/env python

''' Add the approriate dbbact ontology id (term_id) to each dbbact term in OntologyTable
need to run once since we didn't dutomatically set when adding new term
'''

import click

import datetime
from collections import defaultdict

import psycopg2

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "1.0"


def _write_log(logfile, msg):
	ctime = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
	with open(logfile, 'a') as fl:
		fl.write('%s: %s\n' % (ctime, msg))


def _get_term_id(con, cur, term, fail_if_not_there=True, only_dbbact=True):
	'''Get the idx of a given dbBact term description or term_id
	if more than 1 match exists, get the dbBact match

	Parameters
	----------
	con, cur
	term: str
		the term description or term_id (dbbact:XXXX) to look for
	fail_if_not_there: bool, optional
		if True, fail if term does not exist. If false, return None instead
	only_dbbact: bool, optional
		if True, return only IDs for terms in dbbact ontology. If false, return term id for any ontology

	Returns
	-------
	id: int
	'''
	cur.execute("SELECT * FROM ontologytable WHERE term_id=%s", [term])
	if cur.rowcount == 0:
		cur.execute("SELECT * FROM ontologytable WHERE description=%s", [term])
	res = cur.fetchall()
	num_dbbact = 0
	for cres in res:
		if only_dbbact:
			if not cres['term_id'].startswith('dbbact:'):
				continue
		term_id = cres['id']
		num_dbbact += 1
	if num_dbbact == 0:
		if fail_if_not_there:
			if only_dbbact:
				raise ValueError('Term %s not found in dbbact ontology. Found in %d non-dbbact' % (term, len(res)))
			else:
				raise ValueError('Term %s not found' % term)
		else:
			debug(2, 'term %s not found' % term)
			return None
	if num_dbbact > 1:
		raise ValueError('Term %s has >1 (%d) dbBact matches' % (term, num_dbbact))

	debug(2, 'term found with 1 instance in ontologytable. id=%d' % term_id)
	return term_id


def _add_dbbact_term(con, cur, term, create_if_not_exist=True, only_dbbact=True):
	term_id = _get_term_id(con, cur, term, fail_if_not_there=False, only_dbbact=only_dbbact)
	# if parent term is not there, create it
	if term_id is None:
		if not create_if_not_exist:
			raise ValueError('Term %s does not exist, and create_if_not_exist flag is not set' % term)
		debug(3, 'term %s does not exist. Creating' % term)
		cur.execute('INSERT INTO OntologyTable (description) VALUES (%s) RETURNING id', [term])
		term_id = cur.fetchone()[0]
		cur.execute('UPDATE ontologytable SET term_id=%s WHERE id=%s', ['dbbact:%s' % term_id, term_id])
	return term_id


@click.group()
@click.option('--database', type=str, show_default=True, default='dbbact', help='dbbact postgres database name')
@click.option('--port', type=int, show_default=True, default=5432, help='postgres server port')
@click.option('--host', type=str, show_default=True, default=None, help='postgres server host address')
@click.option('--user', type=str, show_default=True, default='dbbact', help='dbbact postgres database user')
@click.option('--password', type=str, show_default=True, default='magNiv', help='dbbact postgres database password')
@click.option('--debug-level', type=int, show_default=True, default=2, help='debug level (1 for debug ... 9 for critical)')
@click.option('--log-file', type=str, show_default=True, default='log-ontology-manager.txt', help='the log file name (for storing history)')
@click.option('--dry-run', type=bool, default=False, show_default=True, help='If set, do not commit to database')
@click.pass_context
def om_cmd(ctx, database, port, host, user, password, debug_level, log_file, dry_run):
	SetDebugLevel(debug_level)
	con, cur = db_access.connect_db(database=database, user=user, password=password, port=port, host=host)
	ctx.obj = {}
	ctx.obj['con'] = con
	ctx.obj['cur'] = cur
	ctx.obj['debug_level'] = debug_level
	ctx.obj['log_file'] = log_file
	ctx.obj['commit'] = not dry_run


@om_cmd.command()
@click.option('--term', '-t', required=True, type=str, help='the term to add')
@click.pass_context
def add_term(ctx, term):
	'''Add a new term to the dbbact ontology
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	term = term.lower()
	debug(3, 'add-term for term %s' % term)
	term_id = _add_dbbact_term(con, cur, term)
	con.commit()
	_write_log(log_file, 'add_term for term: %s (id: %s)' % (term, term_id))


@om_cmd.command()
@click.option('--term', '-t', required=True, type=str, help='the term to delete')
@click.option('--delete-from-annotations', type=bool, default=False, show_default=True, help='If set, enable deletion even if term appears in annotations (and delete the term from the annotations)'))
@click.pass_context
def delete_term(ctx, term, delete_from_annotations):
	'''Delete a term from the ontology table. Cannot delete if it is a parent of another term or if it appears in annotations unless delete-from-annotations is set
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	term = term.lower()
	debug(3, 'delete-term for term %s' % term)
	term_id = _add_dbbact_term(con, cur, term, create_if_not_exist=False, only_dbbact=True)

	# check if it is a parent of someone
	cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyparentid=%s', [term_id])
	if cur.rowcount > 0:
		raise ValueError('The term %s is a parent of %d terms. Cannot delete' % (term, cur.rowcount))

	# check if it appears in annotations
	cur.execute('SELECT idannotation FROM AnnotationListTable WHERE idontology = %s', [term_id])
	if cur.rowcount > 0:
		if not delete_from_annotations:
			raise ValueError('The term %s appears in %d annotations. Cannot delete' % (term, cur.rowcount))
		else:
			res = input('Term appears in %d annotations. Delete %s (%s): Are you sure (y/n)?' % (cur.rowcount, term, term_id))
	else:
		res = input('Delete %s (%s): Are you sure (y/n)?' % (term, term_id))

	if not res.lower() in ('y', 'yes'):
		raise ValueError('Delete aborted')

	# delete the term from the annotations
	cur.execute('DELETE FROM AnnotationListTable WHERE idontology=%s', [term_id])

	# delete all the entries where it is a child
	cur.execute('DELETE FROM ontologytreestructuretable WHERE ontologyid=%s', [term_id])
	# and delete the term itself
	cur.execute('DELETE FROM ontologytable WHERE id=%s', [term_id])
	con.commit()
	_write_log(log_file, 'delete_term for term: %s (id: %s)' % (term, term_id))


@om_cmd.command()
@click.option('--term', '-t', required=True, type=str, help='the term to get info about')
@click.option('--partial', '-p', is_flag=True, default=False, help='search for term as substring')
@click.option('--no-parent', is_flag=True, default=False, help='only list terms that have no parent')
@click.pass_context
def term_info(ctx, term, partial, no_parent):
	'''Get information about a dbBact term
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	term = term.lower()

	debug(3, 'term-info for term %s' % term)
	cur.execute('SELECT * FROM ontologytable WHERE term_id=%s', [term])
	if cur.rowcount == 0:
		if partial:
			cur.execute('SELECT * FROM ontologytable WHERE description LIKE %s', [term + '%'])
		else:
			cur.execute('SELECT * FROM ontologytable WHERE description=%s', [term])
	res = cur.fetchall()
	for cres in res:
		cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyid=%s', [cres['id']])
		skip_it = False
		all_parents = []
		if cur.rowcount > 0:
			parents = cur.fetchall()
			for cparent in parents:
				cur.execute('SELECT * FROM OntologyTable WHERE id=%s LIMIT 1', [cparent['ontologyparentid']])
				cinfo = cur.fetchone()
				all_parents.append('%s (%s)' % (cinfo['description'], cinfo['term_id']))
				if cinfo['term_id'] == 'dbbact:1811274':
					continue
				skip_it = True
		if skip_it:
			if no_parent:
				continue
		print('\n*******************')
		print('TERM: %s (TERM_ID: %s )' % (cres['description'], cres['term_id']))
		print(list(cres.items()))
		print('===================')
		print('PARENTS:')
		for cparent in all_parents:
			print(cparent)
		print('CHILDREN:')
		cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyparentid=%s', [cres['id']])
		children = cur.fetchall()
		for cchild in children:
			cur.execute('SELECT * FROM ontologytable WHERE id=%s LIMIT 1', [cchild['ontologyid']])
			cchilddet = cur.fetchone()
			print(cchilddet['description'])
		annotation_ids = []
		exp_names = set()
		print('ANNOTATIONS:')
		cur.execute('SELECT idannotation FROM AnnotationListTable WHERE idontology = %s', [cres['id']])
		res2 = cur.fetchall()
		for cres2 in res2:
			annotation_ids.append(cres2['idannotation'])
		print('total %d annotations' % len(annotation_ids))
		for canno in annotation_ids:
			cur.execute('SELECT idexp FROM AnnotationsTable WHERE id=%s LIMIT 1', [canno])
			res2 = cur.fetchone()
			cur.execute('SELECT * FROM ExperimentsTable WHERE expid=%s', [res2['idexp']])
			res2 = cur.fetchall()
			for cexp in res2:
				if cexp['type'] != 'name':
					continue
				exp_names.add('%s (expid: %s)' % (cexp['value'], cexp['expid']))
		print('----------------')
		print('Experiments:')
		for cname in exp_names:
			print(cname)


@om_cmd.command()
@click.option('--term', '-t', required=True, type=str, help='the term to link to the parent')
@click.option('--parent', '-p', required=True, type=str, help='the parent term')
@click.option('--add-if-not-exist', default=False, is_flag=True, help='Add the parent term to dBact ontology if does not exist')
@click.option('--old-parent', type=click.Choice(['replace', 'insert', 'ignore', 'fail'], case_sensitive=False), help='if old parent exists, "replace" or "insert" between or "ignore" or "fail"', default='fail', show_default=True)
@click.option('--only-dbbact/--not-only-dbbact', default=True, is_flag=True, help='Add the parent term even if it is not from the dbbact ontology (belongs to a different ontology)')
@click.pass_context
def add_parent(ctx, term, parent, add_if_not_exist, old_parent, only_dbbact):
	'''Link a dbBact ontology term to a dbBact parent term.
	If the parent term does not exist, dbBact creates it
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	commit = ctx.obj['commit']
	log_file = ctx.obj['log_file']
	term = term.lower()
	parent = parent.lower()

	debug(3, 'add parent %s to term %s' % (parent, term))
	term_id = _get_term_id(con, cur, term)
	parent_term_id = _add_dbbact_term(con, cur, parent, create_if_not_exist=add_if_not_exist, only_dbbact=only_dbbact)

	# to be safe, get the dbBact ontology number
	cur.execute('SELECT id FROM ontologynamestable WHERE description=%s', ['dbbact'])
	ontology_database_id = cur.fetchone()[0]
	debug(2, 'dbBact database id=%s' % ontology_database_id)
	if ontology_database_id != 8:
		raise ValueError('dbbact id is not 8! it is %d' % ontology_database_id)

	# check if it had "dbbact root" (id 1811274) as parent - remove it
	cur.execute('DELETE FROM ontologytreestructuretable WHERE ontologynameid=8 AND ontologyparentid=1811274 AND ontologyid=%s', [term_id])

	cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyid=%s', [term_id])
	if cur.rowcount > 0:
		debug(3, 'old parents (%d) found for term' % cur.rowcount)
		if old_parent == 'replace':
			if cur.rowcount > 1:
				raise ValueError('More than 1 parent for term (%d). Cannot replace.' % cur.rowcount)
			# remove the old parent
			res = cur.fetchone()
			cur.execute('DELETE FROM ontologytreestructuretable WHERE uniqueid=%s', [res['uniqueid']])
		elif old_parent == 'insert':
			if cur.rowcount > 1:
				raise ValueError('More than 1 parent for term (%d). Cannot insert.' % cur.rowcount)
			# add our parent term in the middle
			res = cur.fetchone()
			cur.execute('SELECT term_id FROM ontologytable WHERE id=%s', [res['ontologyparentid']])
			idres = cur.fetchone()
			ctx.invoke(add_parent, term=parent, parent=idres['term_id'], add_if_not_exist=False, old_parent='fail')
			# and remove the old parent connection
			cur.execute('DELETE FROM ontologytreestructuretable WHERE uniqueid=%s', [res['uniqueid']])
		elif old_parent == 'ignore':
			debug('term already has parents (%d). Ignoring and adding new parent' % cur.rowcount)
		elif old_parent == 'fail':
			raise ValueError('Parents (%d) already exists for term. To override use the old-parent option' % cur.rowcount)

	# add to the OntologyTreeStructureTable
	cur.execute('INSERT INTO ontologytreestructuretable (ontologyid, ontologyparentid, ontologynameid) VALUES (%s, %s, %s)', [term_id, parent_term_id, ontology_database_id])
	debug(3, 'Inserted into ontologytreestructuretable')
	if commit:
		_write_log(log_file, 'add_parent for term: %s (id: %s) parent: %s (id: %s)' % (term, term_id, parent, parent_term_id))
		con.commit()
	else:
		debug(5, 'dry run - not commiting')
	debug(3, 'done')


@om_cmd.command()
@click.option('--old-term', '-t', required=True, type=str, help='the term to rename')
@click.option('--new-term', '-n', required=True, type=str, help='the new term')
@click.option('--experiments', '-e', multiple=True, default=None, type=int, help='replace only in experiments from the list of these experiment IDs')
@click.option('--add-if-not-exist', default=False, is_flag=True, help='Add the new term to dbBact ontology if does not exist')
@click.option('--ignore-no-annotations', default=False, is_flag=True, help='Rename the term even if does not appear in any annotation')
@click.option('--inplace', default=False, is_flag=True, help='Just change the description of the old term (i.e. change the name for the term instead of creating a new one)')
@click.pass_context
def rename_term(ctx, old_term, new_term, experiments, add_if_not_exist, ignore_no_annotations, inplace):
	'''replace a term with another term in all annotations. If inplace=True, just change the description of the term
	If the new term does not exist, dbBact creates it into the dbbact ontology
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	old_term = old_term.lower()
	new_term = new_term.lower()

	debug(3, 'rename term %s to term %s' % (old_term, new_term))

	# not sure if multiple provides None or [], so let's make it None
	if experiments is not None:
		if len(experiments) == 0:
			experiments = None

	if experiments is not None:
		if inplace:
			raise ValueError('Cannot replcae in place in a subset of experiments.')

	old_term_id = _get_term_id(con, cur, old_term, only_dbbact=False)
	if old_term_id is None:
		raise ValueError('Term %s does not exist' % old_term)

	if inplace:
		cur.execute('SELECT * FROM OntologyTable WHERE description=%s', [new_term])
		if cur.rowcount > 0:
			raise ValueError('new term %s already exists as description' % new_term)
		cur.execute('SELECT * FROM OntologyTable WHERE term_id=%s', [new_term])
		if cur.rowcount > 0:
			raise ValueError('new term %s already exists as term_id' % new_term)
		cur.execute('UPDATE OntologyTable SET description=%s WHERE id=%s', [new_term, old_term_id])
		_write_log(log_file, 'rename_term for old_term: %s (id: %s) to new_term: %s in place' % (old_term, old_term_id, new_term))
		con.commit()
		debug(3, 'done')
		return

	new_term_id = _add_dbbact_term(con, cur, new_term, create_if_not_exist=add_if_not_exist, only_dbbact=False)

	# get all annotations with the old term
	cur.execute('SELECT idannotation FROM AnnotationListTable WHERE idontology=%s', [old_term_id])
	if cur.rowcount == 0:
		if not ignore_no_annotations:
			raise ValueError('No annotations found containing term %s' % old_term)
	debug(3, 'found %d annotations with the term %s' % (cur.rowcount, old_term))

	# update to the new term
	if experiments is None:
		cur.execute('UPDATE AnnotationListTable SET idontology=%s WHERE idontology=%s', [new_term_id, old_term_id])
	else:
		num_match = 0
		match_exps = set()
		non_match_exps = set()
		num_non_match = 0
		experiments = set(experiments)
		annotations = cur.fetchall()
		for cannotation in annotations:
			cannotation_id = cannotation['idannotation']
			cur.execute('SELECT idexp FROM AnnotationsTable WHERE id=%s LIMIT 1', [cannotation_id])
			if cur.rowcount == 0:
				debug(7, 'experiment ID %s not found! skipping' % cannotation_id)
				continue
			res = cur.fetchone()
			if res['idexp'] in experiments:
				num_match += 1
				match_exps.add(res['idexp'])
				cur.execute('UPDATE AnnotationListTable SET idontology=%s WHERE idontology=%s AND idannotation=%s', [new_term_id, old_term_id, cannotation_id])
			else:
				num_non_match += 1
				non_match_exps.add(res['idexp'])
		debug(3, 'found %d annotations (%d experiments) with a matching expid, %d (%d) without' % (num_match, len(match_exps), num_non_match, len(non_match_exps)))

	# update the ontology parents table - only if we did not do a partial update
	if experiments is None:
		cur.execute('SELECT * FROM OntologyTreeStructureTable WHERE ontologyparentid=%s', [old_term_id])
		if cur.rowcount > 0:
			debug(3, 'Found %d terms with %s as parent term. Updating' % (cur.rowcount, old_term))
			res = cur.fetchall()
			for cres in res:
				cur.execute('UPDATE OntologyTreeStructureTable SET ontologyparentid=%s WHERE uniqueid=%s', [new_term_id, cres['uniqueid']])

	_write_log(log_file, 'rename_term for old_term: %s (id: %s) to new_term: %s (id: %s)' % (old_term, old_term_id, new_term, new_term_id))
	con.commit()
	debug(3, 'done')


@om_cmd.command()
@click.option('--old-term', '-t', required=True, type=str, help='the term to rename')
@click.option('--new-term', '-n', required=True, type=str, help='the new term')
@click.option('--experiments', '-e', multiple=True, default=None, type=int, help='replace only in experiments from the list of these experiment IDs')
@click.option('--add-if-not-exist', default=False, is_flag=True, help='Add the new term to dbBact ontology if does not exist')
@click.pass_context
def add_term_to_annotation(ctx, old_term, new_term, experiments, add_if_not_exist):
	'''Add another term to annotations containing a given term
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	old_term = old_term.lower()
	new_term = new_term.lower()

	debug(3, 'add term %s to annotations with term %s' % (old_term, new_term))

	# not sure if multiple provides None or [], so let's make it None
	if experiments is not None:
		if len(experiments) == 0:
			experiments = None
		else:
			experiments = set(experiments)
	old_term_id = _get_term_id(con, cur, old_term, only_dbbact=False)
	if old_term_id is None:
		raise ValueError('Term %s does not exist' % old_term)

	new_term_id = _add_dbbact_term(con, cur, new_term, create_if_not_exist=add_if_not_exist, only_dbbact=False)

	# get all annotations with the old term
	cur.execute('SELECT idannotation,idannotationdetail FROM AnnotationListTable WHERE idontology=%s', [old_term_id])
	if cur.rowcount == 0:
		raise ValueError('No annotations found containing term %s' % old_term)
	debug(3, 'found %d annotations with the term %s' % (cur.rowcount, old_term))

	annotations = cur.fetchall()
	num_added = 0
	num_non_match = 0
	for cannotation in annotations:
		cannotation_id = cannotation['idannotation']
		canntation_detail = cannotation['idannotationdetail']
		if experiments is not None:
			cur.execute('SELECT idexp FROM AnnotationsTable WHERE id=%s LIMIT 1', [cannotation_id])
			if cur.rowcount == 0:
				debug(7, 'experiment ID %s not found! skipping' % cannotation_id)
				num_non_match += 1
				continue
			res = cur.fetchone()
			if res['idexp'] not in experiments:
				continue
		cur.execute('INSERT INTO AnnotationListTable (idannotation, idannotationdetail, idontology) VALUES (%s, %s, %s)', [cannotation_id, canntation_detail, new_term_id])
		num_added += 1
	debug(3, 'added new term to %d annotations (%d annotations skipped)' % (num_added, num_non_match))
	_write_log(log_file, 'add_term_to_annotation for old_term: %s (id: %s) to new_term: %s (id: %s)' % (old_term, old_term_id, new_term, new_term_id))
	con.commit()
	debug(3, 'done')


@om_cmd.command()
@click.option('--term1', '-t', required=True, type=str, help='first term combine')
@click.option('--term2', '-u', required=True, type=str, help='sencond term to combine')
@click.pass_context
def combine_terms(ctx, term1, term2):
	'''Conbine two terms - i.e. add both terms to each annotations containing one of the terms
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	term1 = term1.lower()
	term2 = term2.lower()

	debug(3, 'combine terms %s, %s in all annotations' % (term1, term2))

	term1_id = _get_term_id(con, cur, term1, only_dbbact=False)
	if term1_id is None:
		raise ValueError('Term %s does not exist' % term1)
	term2_id = _get_term_id(con, cur, term2, only_dbbact=False)
	if term2_id is None:
		raise ValueError('Term %s does not exist' % term2)

	# get all annotations with the old and new terms
	cur.execute('SELECT idannotation,idannotationdetail FROM AnnotationListTable WHERE idontology=%s', [term1_id])
	debug(3, 'found %d annotations with the term %s' % (cur.rowcount, term1))
	term1_annotations = cur.fetchall()
	cur.execute('SELECT idannotation,idannotationdetail FROM AnnotationListTable WHERE idontology=%s', [term2_id])
	debug(3, 'found %d annotations with the term %s' % (cur.rowcount, term2))
	term2_annotations = cur.fetchall()
	annotations = term1_annotations.copy()
	annotations.extend(term2_annotations)

	term1_annotation_ids = set([x['idannotation'] for x in term1_annotations])
	term2_annotation_ids = set([x['idannotation'] for x in term2_annotations])

	num_added = 0
	num_non_match = 0
	for cannotation in annotations:
		cannotation_id = cannotation['idannotation']
		canntation_detail = cannotation['idannotationdetail']
		# test if the annotation already contains either of the terms
		if cannotation_id not in term1_annotation_ids:
			cur.execute('INSERT INTO AnnotationListTable (idannotation, idannotationdetail, idontology) VALUES (%s, %s, %s)', [cannotation_id, canntation_detail, term1_id])
		else:
			cur.execute('INSERT INTO AnnotationListTable (idannotation, idannotationdetail, idontology) VALUES (%s, %s, %s)', [cannotation_id, canntation_detail, term2_id])
		num_added += 1

	debug(3, 'added new term to %d annotations (%d annotations skipped)' % (num_added, num_non_match))
	_write_log(log_file, 'combine_terms for term: %s (id: %s) and term: %s (id: %s)' % (term1, term1_id, term2, term2_id))
	con.commit()
	debug(3, 'done')


@om_cmd.command()
@click.pass_context
def find_duplicates(ctx):
	'''Find terms from different ontologies that have the same name (i.e. feces) and list in which annotations they appear
	Ignores duplicates that are from gaz ontology
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']

	cur2 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
	debug(3, 'find duplicate terms')
	num_na = 0
	num_empty = 0
	cur.execute("SELECT * from ontologytable where term_id NOT LIKE 'gaz:%'")
	for crow in cur:
		cterm = crow['description']
		if cterm == 'na':
			num_na += 1
			continue
		if cterm == '':
			num_empty += 1
			continue
		# check if it is a double
		cur2.execute('SELECT * from ontologytable where description=%s', [cterm])
		if cur2.rowcount < 2:
			continue

		# get the ids of the terms
		similar_ids = {}
		similar = set()
		for crow2 in cur2:
			if crow2['term_id'].startswith('gaz:'):
				continue
			similar_ids[crow2['id']] = crow2['term_id']
			similar.add(crow2['term_id'])

		# check if it appears in any annotations
		cur2.execute('SELECT * from AnnotationListTable WHERE idontology IN %s', [tuple(similar_ids)])
		if cur2.rowcount == 0:
			continue
		print('term: %s (%s)' % (cterm, similar))
		annotation_terms = defaultdict(list)
		for crow2 in cur2:
			annotation_terms[crow2['idannotation']].append(similar_ids[crow2['idontology']])
		for ck, cv in annotation_terms.items():
			print(' * annotation: %d terms: %s' % (ck, cv))

	print('num na: %d, num_empty: %d' % (num_na, num_empty))
	debug(3, 'done')


if __name__ == "__main__":
	om_cmd()
	# main(sys.argv[1:])
