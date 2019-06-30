#!/usr/bin/env python

# amnonscript

import argparse
from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access

import sys

__version__ = "1.0"


def delete_unused_seqs(con, cur, delete=False):
	debug(3, 'delete unused seqs started')
	if delete:
		cur.execute('DELETE FROM SequencesTable WHERE NOT EXISTS(SELECT SequencesAnnotationTable.seqid FROM SequencesAnnotationTable WHERE SequencesAnnotationTable.seqid = SequencesTable.id)')
		debug(3, 'deleted')
		con.commit()
	else:
		cur.execute('SELECT * FROM SequencesTable WHERE NOT EXISTS(SELECT SequencesAnnotationTable.seqid FROM SequencesAnnotationTable WHERE SequencesAnnotationTable.seqid = SequencesTable.id)')
		print('NOT DELETING, but found %d sequences to delete' % cur.rowcount)
		debug(3, 'NOT DELETING, but found %d sequences to delete' % cur.rowcount)


def main(argv):
	parser = argparse.ArgumentParser(description='delete_unused_seqs version %s\ndelete sequences not in any annotation' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--delete', help='delete the sequences', action='store_true')

	args = parser.parse_args(argv)
	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	delete_unused_seqs(con, cur, delete=args.delete)


if __name__ == "__main__":
	main(sys.argv[1:])
