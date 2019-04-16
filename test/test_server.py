#!/usr/bin/env python

import subprocess
import time
import argparse
import sys
import atexit

import requests

__version__ = "0.9"
server_addr = '127.0.0.1:5002'

'''To create the dbbact_test database:
# create the user, database, extensions
/Applications/Postgres.app/Contents/MacOS/bin/psql postgres < create_test_db.commands.txt
# relicate the REAL database structure (not the export version... need to fix)
/Applications/Postgres.app/Contents/MacOS/bin/pg_restore -U dbbact_test -d dbbact_test --schema-only --no-owner dbbact-full-2019-04-02.psql
# add user 0 to userstable (anonymous user)
/Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO UsersTable (id,username) VALUES(0,'na');"
'''


def aeq(a, b):
	if a == b:
		return
	raise AssertionError('assert_equal failed. %r != %r' % (a, b))


def ain(elem, group):
	if elem in group:
		return
	raise AssertionError('assert_in failed. %r not in %r' % (elem, group))


def alen(a, num):
	if len(a) == num:
		return
	raise AssertionError('assert_len failed. len of %r is %d and not %d' % (a, len(a), num))


def akv(k, v, d):
	if k in d:
		if d[k] == v:
			return
	raise AssertionError('assert_dict_item failed. dict %r does not cotain key %s value %s' % (d, k, v))


def start_server(out_file_name='./log-test-server.txt'):
	global server_proc

	print('starting server on address %s with output to %s' % (server_addr, out_file_name))
	cmd = ['gunicorn', 'dbbact_server.Server_Main:gunicorn(debug_level=2,server_type="test")', '-b', server_addr, '--workers', '4', '--name=test-dbbact-rest-api', '--timeout', '300']
	# proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	outfile = open(out_file_name, 'w')
	proc = subprocess.Popen(cmd, shell=False, stdout=outfile, stderr=outfile)
	if proc is None:
		raise ValueError('Did not manage to start gunicorn dbbact_server process')
	atexit.register(stop_server, proc=proc, outfile=outfile)
	print('server started on address %s' % server_addr)
	time.sleep(5)
	return proc


def stop_server(proc, outfile):
	proc.terminate()
	outfile.close()


def pget(addr, data=None, should_work=True):
	res = requests.get('http://' + server_addr + '/' + addr, json=data)
	if res.ok != should_work:
		print('got unexpected response %r for pget %s data %s\n%s' % (res, addr, data, res.content))
	if res.ok:
		return res.json()
	return None


def ppost(addr, data=None, should_work=True):
	res = requests.post('http://' + server_addr + '/' + addr, json=data)
	if res.ok != should_work:
		print('got unexpected response %r for pget %s data %s\n%s' % (res, addr, data, res.content))
	if res.ok:
		return res.json()
	return None


def test_server():
	start_server()

	# test user module functions
	print('testing users')
	res = ppost('/users/register_user', {'user': 'test1', 'pwd': 'secret', 'name': 'mr test', 'email': 'test@test.com', 'publish': 'n'})
	res = ppost('/users/register_user', {'user': 'test1', 'pwd': 'secret', 'name': 'mr test', 'email': 'test@test.com', 'publish': 'n'}, should_work=False)
	res = ppost('/users/register_user', {'user': 'test2', 'pwd': 'secret2', 'name': 'mr test2', 'email': 'test2@test.com', 'publish': 'y'})
	res = ppost('/users/get_user_public_information', {'username': 'test1'})
	aeq(res['id'], 1)
	aeq(res['name'], 'mr test')
	aeq(res['email'], '-')
	res = ppost('/users/get_user_public_information', {'username': 'test2'})
	aeq(res['email'], 'test2@test.com')

	# test experiment module functions
	print('testing experiments')
	res = ppost('/experiments/add_details', data={'expID': None, 'details': [('name', 'test1'), ('SRA', 'PRJ1')]})
	res = ppost('/experiments/add_details', data={'expID': None, 'details': [('name', 'test2'), ('SRA', 'PRJ2')], 'user': 'test1', 'pwd': 'secret'})
	res = ppost('/experiments/add_details', data={'expID': None, 'details': [('name', 'test3'), ('SRA', 'PRJ3')], 'user': 'test1', 'pwd': 'wrongpassword'})
	res = pget('/experiments/get_experiments_list')
	alen(res['explist'], 3)
	res = pget('/experiments/get_id', {'details': [('SRA', 'PRJ2')]})
	aeq(res['expId'], [2])
	res = pget('/experiments/get_id', {'details': [('SRA', 'PRJ4')]})
	alen(res['expId'], 0)
	res = pget('/experiments/get_details', {'expId': 2})
	ain(['name', 'test2'], res['details'])
	alen(res['details'], 2)

	# test primers module in sequences
	print('testing primers table')
	res = ppost('/sequences/add_primer_region', {'name': 'V4', 'fprimer': '515f'})
	res = ppost('/sequences/add_primer_region', {'name': 'v34'})
	res = ppost('/sequences/add_primer_region', {'name': 'V4', 'fprimer': '520f'}, should_work=False)
	res = pget('/sequences/get_primers')
	alen(res['primers'], 2)

	# test annotations module
	print('testing annotations')
	res = ppost('/annotations/add', {'expId': 1, 'sequences': ['A' * 150, 'T' * 150], 'region': 'v4', 'annotationType': 'common', 'description': 'test annotation1', 'annotationList': [('all', 'mus musculus')]})
	res = ppost('/annotations/add', {'expId': 1, 'sequences': ['C' * 150, 'C' * 150], 'region': 'v4', 'method': 'magic', 'annotationType': 'common', 'description': 'test annotation2', 'annotationList': [('all', 'feces'), ('all', 'dog')], 'user': 'test1', 'pwd': 'secret'})
	res = ppost('/annotations/add', {'expId': 2, 'sequences': ['a' * 150, 'E' * 150, 'f' * 150], 'region': 'V34', 'annotationType': 'other', 'description': 'test annotation3', 'annotationList': [('all', 'feces'), ('all', 'cat')]})

	res = pget('annotations/get_all_annotations')
	alen(res['annotations'], 3)

	res = pget('annotations/get_annotation', {'annotationid': 2})
	aeq(res['userid'], 1)
	aeq(res['username'], 'test1')
	aeq(res['expid'], 1)
	aeq(res['annotationtype'], 'common')
	aeq(res['method'], 'magic')
	aeq(res['agent'], 'na')
	aeq(res['num_sequences'], 1)
	ain(['all', 'dog'], res['details'])
	alen(res['details'], 2)

	res = pget('/annotations/get_full_sequences', {'annotationid': 1})
	alen(res['sequences'], 2)
	for cres in res['sequences']:
		if cres['seq'] == 'a' * 150:
			break
	akv('seq', 'a' * 150, cres)
	akv('taxonomy', '', cres)

	res = pget('annotations/get_sequences', {'annotationid': 3})
	alen(res['seqids'], 3)

	res = pget('/sequences/get_annotations', {'sequence': 'A' * 150})
	alen(res['annotations'], 2)
	res = pget('/sequences/get_annotations', {'sequence': 'A' * 120})
	alen(res['annotations'], 2)
	res = pget('/sequences/get_annotations', {'sequence': 'F' * 160})
	alen(res['annotations'], 1)
	res = pget('/sequences/get_annotations', {'sequence': 'A' * 120, 'region': 'pita'})
	alen(res['annotations'], 0)

	res = pget('stats/stats')
	print('all tests completed ok')
	print('database stats: %s' % res)


def main(argv):
	global server_addr

	print('test_server.py started')
	parser = argparse.ArgumentParser(description='test_server version %s' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--server-addr', help='address of the test server', default='127.0.0.1:5002')
	args = parser.parse_args(argv)
	server_addr = args.server_addr
	test_server()


if __name__ == "__main__":
	main(sys.argv[1:])
