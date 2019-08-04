#!/usr/bin/env python

import argparse
import sys
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

import setproctitle

from dbbact_server.utils import debug, SetDebugLevel

__version__ = "0.1"


commands = {'update_seq_taxonomy': './update_seq_taxonomy.py',
			'update_seq_hash': './update_seq_hash.py',
			'update_term_info': './update_term_info.py',
			# 'update_silva': './update_whole_seq_db.py -w SILVA',
			# 'update_gg': './update_whole_seq_db.py -w greengenes',
			# 'update_seq_translator': './update_whole_seq_db.py --server-type develop --wholeseqdb silva --wholeseq-file ~/whole_seqs/SILVA_132_SSURef_tax_silva.fasta',
			'update_seq_translator': './update_whole_seq_db.py --wholeseqdb silva',
			'update_seq_counts': './update_seq_counts.py'}


def get_time_to_tomorrow(hour, minute=0):
	x = datetime.today()
	y = datetime.today() + timedelta(days=1)
	y = y.replace(hour=hour, minute=minute, second=0, microsecond=0)
	delta_t = y - x
	secs = delta_t.seconds + 1
	debug(1, '%d seconds until tomorrow %d:%d:00' % (secs, hour, minute))
	return secs


def isFileExist(fileName):
	my_file = Path(fileName)
	if my_file.is_file():
		# file exists
		return True
	return False


def removeFile(file_name):
	try:
		os.remove(file_name)
	except OSError:
		pass


def run_bg_jobs(port, host, database, user, password, single_update=False, command_params=None, debug_level=None, output_dir=None, proc_title=None):
	debug(3, 'run_bg_jobs started')
	if single_update:
		debug(3, 'running single_update and quitting')
	cpath = os.path.abspath(__file__)
	cdir = os.path.dirname(cpath)
	debug(2, 'path for commands is: %s. output dir is %s' % (cdir, output_dir))
	stop_file = "stop.run_bg_jobs"
	removeFile(stop_file)
	while not isFileExist(stop_file):
		for idx, (ccommand, cbash) in enumerate(commands.items()):
			cuser = user
			cpassword = password
			cdatabase = database
			cport = port
			if command_params is not None:
				for cpar in command_params:
					cpar_split = cpar.split(':')
					if len(cpar_split) != 3:
						debug(5, 'command parameters %s should be command:param_name:value' % cpar)
						continue
					if ccommand == cpar_split[0]:
						if cpar_split[1] == 'user':
							cuser = cpar_split[2]
							continue
						if cpar_split[1] == 'database':
							cdatabase = cpar_split[2]
							continue
						if cpar_split[1] == 'port':
							cport = cpar_split[2]
							continue
						if cpar_split[1] == 'password':
							cpassword = cpar_split[2]
							continue
						cbash += ' --%s %s' % (cpar_split[1], cpar_split[2])
			cbash += ' --port %s --database %s --user %s --password %s' % (cport, cdatabase, cuser, cpassword)
			if host is not None:
				cbash += ' --host %s' % host
			if debug_level is not None:
				cbash += ' --debug-level %d' % debug_level
			if proc_title is not None:
				cbash += ' --proc-title "%s [%s]"' % (proc_title, ccommand)
			cbash = os.path.join(cdir, cbash)
			debug(2, 'running command %s (%d / %d)' % (ccommand, idx + 1, len(commands)))
			debug(1, cbash)
			if output_dir is None:
				output_file = 'log-%s.txt' % ccommand
			else:
				output_file = os.path.join(output_dir, 'log-%s.txt' % ccommand)
			with open(output_file, 'a') as logfile:
				start_time = time.time()
				res = subprocess.call(cbash, shell=True, stderr=logfile, stdout=logfile)
				end_time = time.time()
				if res != 0:
					debug(5, 'command %s failed. error code: %s' % (ccommand, res))
				else:
					debug(2, 'command exited ok. running time: %r sec' % (end_time - start_time))
				# check for sig_term so we will stop running
				if res == -15:
					debug(8, 'sigterm encountered. exiting')
					raise ValueError('sigterm encountered')
		if single_update:
			debug(3, 'single_update - finished')
			break
		debug(2, 'sleeping until tomorrow')
		time.sleep(get_time_to_tomorrow(23, 0))
		debug(2, 'good morning')
	debug(3, 'run_bg_jobs finished')


def main(argv):
	parser = argparse.ArgumentParser(description='run_bg_jobs version %s.' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database (overrides value from server-type)')
	parser.add_argument('--user', help='postgres user (overrides value from server-type)')
	parser.add_argument('--password', help='postgres password (overrides value from server-type)')
	parser.add_argument('--server-type', help='dbbact rest api server type (main/develop/test)', default='main')
	parser.add_argument('-o', '--output-dir', help='output directory for the log files')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
	parser.add_argument('--single-update', help='update once and quit', action='store_true')
	parser.add_argument('-p', '--command-params', help='specific command parameters. command and parameter name separated by : (i.e. update_silva:wholeseq-file:SILVA.fa). can use flag more than once', action='append')
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)
	server_type = args.server_type
	database = args.database
	user = args.user
	password = args.password
	proc_title = 'dbbact run_bg_jobs.py'
	if server_type == 'main':
		proc_title += ' [main]'
		if database is None:
			database = 'dbbact'
		if user is None:
			user = 'dbbact'
		if password is None:
			password = 'magNiv'
	elif server_type == 'develop':
		proc_title += ' [develop]'
		if database is None:
			database = 'dbbact_develop'
		if user is None:
			user = 'dbbact_develop'
		if password is None:
			password = 'dbbact_develop'
	elif server_type == 'test':
		proc_title += ' [test]'
		if database is None:
			database = 'dbbact'
		if user is None:
			user = 'dbbact'
		if password is None:
			password = 'magNiv'
	elif server_type is None:
		pass
	else:
		raise ValueError('unknown server-type. should be one of ("main" / "develop" / "test"')

	setproctitle.setproctitle(proc_title + ' [master]')
	run_bg_jobs(port=args.port, host=args.host, database=database, user=user, password=password, single_update=args.single_update, command_params=args.command_params, debug_level=args.debug_level, output_dir=args.output_dir, proc_title=proc_title)


if __name__ == "__main__":
	SetDebugLevel(1)
	main(sys.argv[1:])
