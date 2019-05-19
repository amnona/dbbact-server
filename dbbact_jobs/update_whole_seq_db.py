#!/usr/bin/env python

import argparse
import sys
from collections import defaultdict

import setproctitle
import psycopg2

from dbbact_sequence_translator.utils import debug, SetDebugLevel
from dbbact_sequence_translator import db_access
from dbbact_sequence_translator import db_translate


__version__ = 0.9


def iter_fasta_seqs(filename):
	"""
	iterate a fasta file and return header,sequence
	input:
	filename - the fasta file name

	output:
	seq - the sequence
	header - the header
	"""

	fl = open(filename, "rU")
	cseq = ''
	chead = ''
	for cline in fl:
		if cline[0] == '>':
			if chead:
				yield(cseq.lower(), chead)
			cseq = ''
			chead = cline[1:].rstrip()
		else:
			cline = cline.strip().lower()
			cline = cline.replace('u', 't')
			cseq += cline.strip()
	if cseq:
		yield(cseq, chead)
	fl.close()


def iter_new_seqs(con):
	"""
	iterate all new sequences in NewSequencesTable
	input:
	con: the database connection

	output:
	seq - the sequence
	header - the header
	"""
	cur2 = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur2.execute('SELECT * FROM NewSequencesTable')
	for cres in cur2:
		cid = cres['dbbactid']
		cseq = cres['sequence']
		yield(cseq, cid)


def hash_sequences(con, cur, dbidVal, whole_seq_db_version=None, short_len=100, check_exists=True):
	'''hash all the sequences in a fasta file

	Parameters
	----------
	con, cur: database connection
	short_len: int, optional
		the minimal sequence length in the database
	check_exists: bool, optional
		True to test and skip sequences already in the WholeSeqIDsTable (assuming SILVA and dbBact sequences have not been updated)

	Returns
	-------
	seq_hash: dict of {seq: seqid}
	seq_lens : list of int
		all the sequence lengths in the fasta file (so we can hash all the lengths in the queries)
	short_hash: dict of {short_seq: seq_hash dict}
	'''
	total_seqs = 0
	num_too_short = 0
	seq_hash = {}
	seq_lens = set()
	all_ids = set()
	short_hash = defaultdict(dict)

	# get the sequence database index from name
	debug(2, 'Scanning dbbact sequences')
	for cseq, cid in iter_new_seqs(con):
		total_seqs += 1
		clen = len(cseq)
		if clen < short_len:
			num_too_short += 1
			continue
		if check_exists:
			err, existsFlag = db_translate.test_whole_seq_id_exists(con, cur, dbidVal=dbidVal, dbbactidVal=cid)
			if existsFlag:
				continue
		all_ids.add(cid)
		short_seq = cseq[:short_len]
		short_hash[short_seq][cseq] = cid
		if clen not in seq_lens:
			seq_lens.add(clen)
		seq_hash[cseq] = cid

	debug(2, 'processed %d dbbact sequences. found %d new sequences' % (total_seqs, len(seq_hash)))
	debug(2, 'lens: %s' % seq_lens)
	debug(2, 'num too short: %d' % num_too_short)
	return all_ids, seq_hash, seq_lens, short_hash


def update_sequencestosequences_table(con, cur, whole_seq_id, whole_seq_db_id, dbbact_id):
	'''
	'''
	# debug(2, 'update_sequencestosequences_table')
	cur.execute('SELECT sequence FROM SequenceIDsTable WHERE WholeSeqID=%s', [whole_seq_id])
	# debug(2, 'found %d matches for wholeseqid %s' % (cur.rowcount, whole_seq_id))
	res = cur.fetchall()
	for sequence_id_res in res:
		cseq = sequence_id_res['sequence']
		cur.execute('SELECT dbbactIDs FROM SequenceToSequenceTable WHERE sequence=%s', [cseq])
		# if doesn't exist - create
		if cur.rowcount == 0:
			# debug(2, 'sequence %s not found in sequencetosequencetable - creating' % cseq)
			cur.execute('INSERT INTO SequenceToSequenceTable (sequence, dbbactIDs) VALUES (%s, %s)', [cseq, str(dbbact_id)])
		else:
			res2 = cur.fetchall()
			# debug(2, 'found %d ids for sequence %s' % (cur.rowcount, cseq))
			for cseq_to_seq_res in res2:
				cdbbact_ids = cseq_to_seq_res['dbbactids']
				cdbbact_ids = set(cdbbact_ids.split(','))
				if str(dbbact_id) in cdbbact_ids:
					continue
				cdbbact_ids.add(str(dbbact_id))
				cur.execute('UPDATE SequenceToSequenceTable SET dbbactIDs=%s WHERE sequence=%s', [','.join(cdbbact_ids), cseq])
				# debug(2, 'updated')
	# debug(2, 'finished')


def update_whole_seq_db(con, cur, whole_seq_fasta_name, seqdbname, check_exists=True, short_len=150, no_delete=False):
	'''
	con, cur:
		connection to the sequence translator database
	whole_seq_fasta_name: str
		name of the whole sequences fasta file (SILVA/GG) of full length 16S sequences
	seqdbname: str
		name of the database we're updating (i.e. 'SILVA' / 'GG')
	check_exists: bool, optional
		True (default) to just update new sequences in wholeseqidstable.
		False to delete all entries in wholeseqidstable prior to processing
	short_len: int, optional
		used to calculate the hashes for the matching to the whole sequence fasta file
	no_delete: bool, optional
		False (default) to delete each entry from the NewSequencesTable after procesing.
		True to keep in table (if multiple processing steps are needed (i.e. SILVA+GG), delete only after last step)
	'''
	debug(3, 'update_whole_seq_db started for database %s' % seqdbname)

	count_success = 0
	count_failure = 0
	count_seq_success = 0
	count_seq_failure = 0
	# set of sequences for which we found at least one match.
	# for all sequences without a match, we add an entry into WholeSeqIDTable with an index 'na' in wholeseqid so we won't scan them next time
	found_seqs = set()

	# silvaFileName = 'SILVA_132_SSURef_tax_silva.fasta'

	debug(2, 'getting id for database %s' % seqdbname)
	err, whole_seq_dbid = db_translate.get_whole_seq_db_id_from_name(con, cur, whole_seq_db_name=seqdbname)
	if err:
		debug(5, 'failed to find database id for %s: %s' % (seqdbname, err))
		raise ValueError(err)
	debug(2, 'found database id %s' % whole_seq_dbid)

	if not check_exists:
		debug(2, 'deleting all entries from WholeSeqIDTable for database %s' % seqdbname)
		cur.execute('DELETE FROM WholeSeqIDsTable WHERE dbid=%s', [whole_seq_dbid])

	# prepare the dbbact sequences to get ids for
	all_ids, seq_hash, seq_lens, short_hash = hash_sequences(con, cur, dbidVal=whole_seq_dbid, short_len=150, check_exists=check_exists)
	if len(all_ids) == 0:
		debug(2, "no sequences to process.")
		return

	debug(2, 'Getting whole seq db IDs for %d dbbact sequences' % len(all_ids))
	# iterate over the silva database file and look for matches to any dbbact sequence
	idx = 0
	for cseq, chead in iter_fasta_seqs(whole_seq_fasta_name):
		isFound = False
		idx += 1
		if idx % 1000 == 1:
			debug(2, "count: %d" % idx)

		for cpos in range(len(cseq) - short_len):
			ccseq = cseq[cpos:cpos + short_len]
			if ccseq in short_hash:
				for k, v in short_hash[ccseq].items():
					if k in cseq:
						# we have an exact match!
						# lets prepare the id string (sometimes has format 'ID.START.END TAXONOMY' we need to remove)
						cid = chead.split(' ')[0]
						split_cid = cid.split('.')
						if len(split_cid) > 2:
							cid = ".".join(split_cid[:-2])
						else:
							cid = ".".join(split_cid)
						cid = cid.lower()
						found_seqs.add(v)

						# update the SequenceToSequenceTable
						update_sequencestosequences_table(con, cur, whole_seq_id=cid, whole_seq_db_id=whole_seq_dbid, dbbact_id=v)
						# add to WholeSeqIDsTable if not already there
						err = db_translate.add_whole_seq_id(con, cur, dbidVal=whole_seq_dbid, dbbactidVal=v, wholeseqidVal=cid, commit=False)
						if err:
							count_seq_failure += 1
							break
						else:
							count_seq_success += 1
							isFound = True
							break
		if isFound:
			count_success += 1

	# now add an entry with wholeseqid = 'na' for all dbbact sequences not matching any WholeSeqID entries
	debug(2, 'found silvaIDs for %d sequences. encountered %d errors.' % (count_success, count_failure))
	debug(2, 'found matches for %s dbbact sequences. adding "na" to all non-matched sequences' % len(found_seqs))
	for cseqid in all_ids:
		if cseqid not in found_seqs:
			err = db_translate.add_whole_seq_id(con, cur, dbidVal=whole_seq_dbid, dbbactidVal=cseqid, wholeseqidVal='na', commit=False)
	if no_delete:
		debug(3, 'skipping delete step')
	else:
		debug(2, 'deleting all new sequences from queue')
		cur.execute('DELETE FROM NewSequencesTable')
	debug(2, 'commiting')
	con.commit()
	debug(2, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='update_whole_seq_db version %s.\nProcess all sequences in the waiting queue table NewSequencesTable.\nShould be run daily.' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--server-type', help='server type (develop/main/test). overridden by --database/user/password', default='main')
	parser.add_argument('--database', help='postgres database')
	parser.add_argument('--user', help='postgres user (to override --server-type)')
	parser.add_argument('--password', help='postgres password (to override --server-type)')
	parser.add_argument('--proc-title', help='name of the process (to view in ps aux)')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
	parser.add_argument('--no-delete', help='do not delete from new sequences queue (NewSequencesTable).', action='store_true')

	parser.add_argument('-w', '--wholeseqdb', help='name of the whole sequence database (i.e. SILVA/GREENGENES)', default='SILVA')
	parser.add_argument('-f', '--wholeseq-file', help='name of the whole sequence fasta file', required=True)
	parser.add_argument('--update-all', help="update all dbbact sequences (recalculate). If not set, will just update new dbbact sequences", action='store_true')
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)
	# set the process name for ps aux
	if args.proc_title:
		setproctitle.setproctitle(args.proc_title)

	# get the database connection
	con, cur = db_access.connect_translator_db(server_type=args.server_type, database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)

	update_whole_seq_db(con, cur, args.wholeseq_file, seqdbname=args.wholeseqdb, check_exists=not args.update_all, no_delete=args.no_delete)


if __name__ == "__main__":
	main(sys.argv[1:])
