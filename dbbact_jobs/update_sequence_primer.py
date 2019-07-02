#!/usr/bin/env python

# amnonscript

import argparse
from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access
from dbbact_server.dbsequences import GetSequenceId
from dbbact_server.dbprimers import GetIdFromName
import sys

__version__ = "1.0"


def update_old_primer_seqs(con, cur, old_primer, new_primer, commit=True):
	'''update all sequences with primer old_primer to new primer new_primer
	'''
	cur.execute("SELECT sequence FROM SequencesTable WHERE idprimer=%s", [old_primer])
	debug(3, 'found %d sequences with old primer %d' % (cur.rowcount, old_primer))
	seqs = []
	res = cur.fetchall()
	for cres in res:
		seqs.append(cres['sequence'])

	for cseq in seqs:
		update_sequence_primer(con, cur, sequence=cseq, primer=new_primer, commit=False)
	if commit:
		con.commit()


def update_sequence_primer(con, cur, sequence, primer, commit=True):
	'''Update the primer region for the sequence.
	If the sequence already appears in dbBact with a different primer region, merge the two using the other region sequence

	Parameters
	----------
	con, cur:
	sequence: str
		the exact sequence to update (acgt)
	primer: int or str
		the primer region id (int) or name (str - i.e. 'v4') to update
	commit: bool, optional
		if True, commit after update

	Returns
	-------
	error (str) or ''
	'''
	debug(2, 'update_sequence_primer for sequence %s new region %s' % (sequence, primer))
	# setup the primer to be the id
	if not isinstance(primer, int):
		primer = GetIdFromName(con, cur, primer)
	# get the sequence id. Note we use idprimer=None since we don't want to look for the new region
	err, seqids = GetSequenceId(con, cur, sequence=sequence, idprimer=None, no_shorter=True, no_longer=True, seq_translate_api=None)
	if err:
		return err
	debug(2, 'found %d total matches to the sequence' % len(seqids))
	if len(seqids) == 0:
		msg = 'trying to update sequence %s failed since it is not in SequencesTable' % sequence
		debug(4, msg)
		return msg
	# do we also have the same sequence with the correct primer?
	err, okid = GetSequenceId(con, cur, sequence=sequence, idprimer=primer, no_shorter=True, no_longer=True, seq_translate_api=None)
	if err:
		if err != 'primer mismatch':
			debug(5, err)
			return err
	# no region matches so choose the first, update it, and move all the others to it
	if len(okid) == 0:
		debug(1, 'could not find sequence with good region. chose seqid %d and updating it' % seqids[0])
		okid = seqids[0]
		cur.execute('UPDATE SequencesTable SET idprimer=%s WHERE id=%s', [primer, okid])
	else:
		debug(3, 'found good sequence id %s. transferring annotations to id' % okid)
		if len(okid) > 1:
			debug(4, 'strange. found %d exact matches including region' % len(okid))
		okid = okid[0]
	# now transfer all annotations from the wrong region sequence to the ok (match) sequence and delete the wrong region sequences
	for cseqid in seqids:
		if cseqid == okid:
			continue
		debug(4, 'moving seqid %d to ok sequence %d and deleting' % (cseqid, okid))
		cur.execute('UPDATE SequencesAnnotationTable SET seqid=%s WHERE seqid=%s', [okid, cseqid])
		cur.execute('DELETE FROM SequencesTable WHERE id=%s', [cseqid])
	if commit:
		debug(3, 'committing')
		con.commit()
	debug(1, 'update finished')
	return ''


def main(argv):
	parser = argparse.ArgumentParser(description='update_sequence_primer.py version %s\nupdate the sequence primer (merge if needed)' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--old-primer', help='the primerid of sequences to update', type=int, default=2)
	parser.add_argument('--new-primer', help='the primerid to update to', type=int, default=1)
	parser.add_argument('--log-level', help='output level (1 verbose, 10 error)', type=int, default=3)
	parser.add_argument('--delete', help='delete the sequences', action='store_true')

	args = parser.parse_args(argv)
	SetDebugLevel(args.log_level)
	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
	update_old_primer_seqs(con, cur, old_primer=args.old_primer, new_primer=args.new_primer, commit=args.delete)


if __name__ == "__main__":
	main(sys.argv[1:])
