#!/usr/bin/env python

import subprocess
import time
import argparse
import sys
import atexit

import requests

server_addr = '127.0.0.1:5002'

'''To create the dbbact_test database:
# create the user, database, extensions
/Applications/Postgres.app/Contents/MacOS/bin/psql postgres < create_test_db.commands.txt
# relicate the REAL database structure (not the export version... need to fix)
/Applications/Postgres.app/Contents/MacOS/bin/pg_restore -U dbbact_test -d dbbact_test --schema-only --no-owner dbbact-full-2019-04-02.psql
# add user 0 to userstable (anonymous user)
/Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO UsersTable (id,username) VALUES(0,'na');"
'''


def start_server(out_file_name='./test_server_out.txt'):
	global server_proc

	cmd = ['gunicorn', 'dbbact_server.Server_Main:gunicorn(debug_level=1,server_type="test")', '-b', server_addr, '--workers', '4', '--name=dbbact-rest-api', '--timeout', '300']
	# proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	print('writing dbbact_server output to file %s' % out_file_name)
	outfile = open(out_file_name, 'w')
	proc = subprocess.Popen(cmd, shell=False, stdout=outfile, stderr=outfile)
	if proc is None:
		raise ValueError('Did not manage to start gunicorn dbbact_server process')
	atexit.register(stop_server, proc=proc, outfile=outfile)
	time.sleep(0.5)
	return proc


def stop_server(proc, outfile):
	proc.terminate()
	outfile.close()


def pget(addr, data=None, should_work=True):
	res = requests.get('http://' + server_addr + '/' + addr, json=data)
	if res.ok != should_work:
		print('got unexpected response %s for pget %s data %s' % (res, addr, data))
	if res.ok:
		return res.json()
	return None


def ppost(addr, data=None, should_work=True):
	res = requests.post('http://' + server_addr + '/' + addr, json=data)
	if res.ok != should_work:
		print('got unexpected response %s for pget %s data %s' % (res, addr, data))
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
	assert(res['id'] == 1)
	assert(res['name'] == 'mr test')
	assert(res['email'] == '-')
	res = ppost('/users/get_user_public_information', {'username': 'test2'})
	assert(res['email'] == 'test2@test.com')

	# test experiment module functions
	print('testing experiments')
	res = ppost('/experiments/add_details', data={'expID': None, 'details': [('name', 'test1'), ('SRA', 'PRJ1')]})
	res = ppost('/experiments/add_details', data={'expID': None, 'details': [('name', 'test2'), ('SRA', 'PRJ2')], 'user': 'test1', 'pwd': 'secret'})
	res = ppost('/experiments/add_details', data={'expID': None, 'details': [('name', 'test3'), ('SRA', 'PRJ3')], 'user': 'test1', 'pwd': 'wrongpassword'})
	res = pget('/experiments/get_experiments_list')
	assert(len(res['explist']) == 3)
	res = pget('/experiments/get_id', {'details': [('SRA', 'PRJ2')]})
	assert(res['expId'] == [2])
	res = pget('/experiments/get_id', {'details': [('SRA', 'PRJ4')]})
	assert(len(res['expId']) == 0)

	res = pget('stats/stats')
	print(res)


def main(argv):
	parser = argparse.ArgumentParser(description='generic version ')
	parser.add_argument('-i', '--input', help='name of input fasta file')
	parser.add_argument('-k', '--keep_primers', help="Don't remove the primer sequences", action='store_true')

	args = parser.parse_args(argv)
	test_server()


if __name__ == "__main__":
	main(sys.argv[1:])
