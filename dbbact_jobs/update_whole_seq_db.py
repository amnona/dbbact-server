#!/usr/bin/env python

import argparse
import sys
from collections import defaultdict

import setproctitle

from dbbact_server.utils import debug, SetDebugLevel
from dbbact_server import db_access
from dbbact_server import dbsequences


__version__ = 0.9


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
	cur2 = con.cursor()
	cur2.execute('SELECT id, sequence FROM SequencesTable')
	for cres in cur2:
		total_seqs += 1
		cid = cres[0]
		cseq = cres[1]
		clen = len(cseq)
		if clen < short_len:
			num_too_short += 1
			continue
		if check_exists:
			err, existsFlag = dbsequences.WholeSeqIdExists(con, cur, dbidVal=dbidVal, dbbactidVal=cid)
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


def update_whole_seq_db(con, cur, seqdbname, whole_seq_fasta_name, check_exists=True, short_len=150):
	'''
	**kwargs:
		server_type=None, database=None, user=None, password=None, port=None, host=None
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
	err, whole_seq_dbid = dbsequences.get_whole_seq_db_id_from_name(con, cur, whole_seq_db_name=seqdbname)
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
		debug(2, "no sequences tp process.")
		return

	debug(2, 'Getting whole seq db IDs for %d dbbact sequences' % len(all_ids))
	# iterate over the silva database file and look for matches to any dbbact sequence
	idx = 0
	for cseq, chead in iter_fasta_seqs(whole_seq_fasta_name):
		isFound = False
		idx += 1
		if idx % 1000 == 0:
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

						# add to dbbact WholeSeqIDsTable if not already there
						err = dbsequences.AddWholeSeqId(con, cur, whole_seq_dbid, v, cid, commit=False)
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
			err = dbsequences.AddWholeSeqId(con, cur, dbidVal=whole_seq_dbid, dbbactidVal=cseqid, wholeseqidVal='na', commit=False)
	debug(2, 'commiting')
	con.commit()
	debug(2, 'done')


def main(argv):
	parser = argparse.ArgumentParser(description='update_whole_seq_db version %s' % __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--port', help='postgres port', default=5432, type=int)
	parser.add_argument('--host', help='postgres host', default=None)
	parser.add_argument('--database', help='postgres database', default='dbbact')
	parser.add_argument('--user', help='postgres user', default='dbbact')
	parser.add_argument('--password', help='postgres password', default='magNiv')
	parser.add_argument('--proc-title', help='name of the process (to view in ps aux)')
	parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)

	parser.add_argument('-f', '--wholeseq-file', help='name of the whole sequence fasta file', required=True)
	parser.add_argument('-w', '--wholeseqdb', help='name of the whole sequence database (i.e. SILVA/GREENGENES)', default='SILVA')
	parser.add_argument('--update-all', help="update all dbbact sequences (recalculate). If not set, will just update new dbbact sequences", action='store_true')
	args = parser.parse_args(argv)

	SetDebugLevel(args.debug_level)
	# set the process name for ps aux
	if args.proc_title:
		setproctitle.setproctitle(args.proc_title)

	# get the database connection
	con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)

	update_whole_seq_db(con, cur, seqdbname=args.wholeseqdb, whole_seq_fasta_name=args.wholeseq_file, check_exists=not args.update_all)


if __name__ == "__main__":
	main(sys.argv[1:])
