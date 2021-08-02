#!/usr/bin/env python

''' Add the approriate dbbact ontology id (term_id) to each dbbact term in OntologyTable
need to run once since we didn't dutomatically set when adding new term
'''

import click

import datetime

from dbbact_server import db_access
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.1"


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
@click.pass_context
def om_cmd(ctx, database, port, host, user, password, debug_level, log_file):
	con, cur = db_access.connect_db(database=database, user=user, password=password, port=port, host=host)
	ctx.obj = {}
	ctx.obj['con'] = con
	ctx.obj['cur'] = cur
	ctx.obj['debug_level'] = debug_level
	ctx.obj['log_file'] = log_file
	SetDebugLevel(debug_level)


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
@click.option('--term', '-t', required=True, type=str, help='the term to get info about')
@click.option('--partial', '-p', is_flag=True, default=False, help='search for term as substring')
@click.option('--no-parent', is_flag=True, default=False, help='only list terms that have no parent')
@click.pass_context
def get_term_info(ctx, term, partial, no_parent):
	'''Get information about a dbBact term
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	term = term.lower()

	debug(3, 'get_-term-info for term %s' % term)
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
		annotation_ids = []
		exp_names = set()
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
@click.pass_context
def add_parent(ctx, term, parent, add_if_not_exist):
	'''Link a dbBact ontology term to a dbBact parent term.
	If the parent term does not exist, dbBact creates it
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	term = term.lower()
	parent = parent.lower()

	debug(3, 'add parent %s to term %s' % (parent, term))
	term_id = _get_term_id(con, cur, term)
	parent_term_id = _add_dbbact_term(con, cur, parent, create_if_not_exist=add_if_not_exist)

	# to be safe, get the dbBact ontology number
	cur.execute('SELECT id FROM ontologynamestable WHERE description=%s', ['dbbact'])
	ontology_database_id = cur.fetchone()[0]
	debug(2, 'dbBact database id=%s' % ontology_database_id)
	if ontology_database_id != 8:
		raise ValueError('dbbact id is not 8! it is %d' % ontology_database_id)

	# check if it had dbbact root as parent - remove it
	cur.execute('DELETE FROM ontologytreestructuretable WHERE ontologynameid=8 AND ontologyparentid=1811274 AND ontologyid=%s', [term_id])

	# add to the OntologyTreeStructureTable
	cur.execute('INSERT INTO ontologytreestructuretable (ontologyid, ontologyparentid, ontologynameid) VALUES (%s, %s, %s)', [term_id, parent_term_id, ontology_database_id])
	debug(3, 'Inserted into ontologytreestructuretable')
	_write_log(log_file, 'add_parent for term: %s (id: %s) parent: %s (id: %s)' % (term, term_id, parent, parent_term_id))
	con.commit()
	debug(3, 'done')


@om_cmd.command()
@click.option('--old-term', '-t', required=True, type=str, help='the term to rename')
@click.option('--new-term', '-n', required=True, type=str, help='the new term')
@click.option('--add-if-not-exist', default=False, is_flag=True, help='Add the new term to dbBact ontology if does not exist')
@click.pass_context
def rename_term(ctx, old_term, new_term, add_if_not_exist):
	'''Link a dbBact ontology term to a dbBact parent term.
	If the parent term does not exist, dbBact creates it
	'''
	con = ctx.obj['con']
	cur = ctx.obj['cur']
	log_file = ctx.obj['log_file']
	old_term = old_term.lower()
	new_term = new_term.lower()

	debug(3, 'rename term %s to term %s' % (old_term, new_term))
	old_term_id = _get_term_id(con, cur, old_term, only_dbbact=False)
	if old_term_id is None:
		raise ValueError('Term %s does not exist' % old_term)
	new_term_id = _add_dbbact_term(con, cur, new_term, create_if_not_exist=add_if_not_exist, only_dbbact=False)

	# get all annotations with the old term
	cur.execute('SELECT idannotation FROM AnnotationListTable WHERE idontology=%s', [old_term_id])
	if cur.rowcount == 0:
		raise ValueError('No annotations found containing term %s' % old_term)
	debug(3, 'found %d annotations with the term %s' % (cur.rowcount, old_term))

	# update to the new term
	cur.execute('UPDATE AnnotationListTable SET idontology=%s WHERE idontology=%s', [new_term_id, old_term_id])

	_write_log(log_file, 'rename_term for old_term: %s (id: %s) to new_term: %s (id: %s)' % (old_term, old_term_id, new_term, new_term_id))
	con.commit()
	debug(3, 'done')


if __name__ == "__main__":
	om_cmd()
	# main(sys.argv[1:])
