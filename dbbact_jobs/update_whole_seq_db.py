#!/usr/bin/env python

import argparse
import csv
import io
import sys
import tempfile
from collections import defaultdict

import setproctitle

from dbbact_sequence_translator.utils import debug, SetDebugLevel
from dbbact_sequence_translator import db_access
from dbbact_sequence_translator import db_translate


'''Process all sequences in the NewSequencesTable queue, so we will have fast wholeseq<->dbBact sequence lookup

Performed by scanning once the whole_seq fasta file (i.e. silva_one_line.fa) and matching each queued dbbact sequence to it.
The matches are collected in a temp file during the scan, and then bulk loaded to update the WholeSeqIDsTable and SequenceToSequenceTable.
'''

__version__ = 0.9


def iter_fasta_seqs(filename, replace_u_t=True):
	"""
	iterate a fasta file and return header,sequence

	Parameters
	----------
	filename: str
		the fasta file name
	replace_u_t: bool, optional
		True to replace 'u' with 't' (e.g. for SILVA fasta file)

	Yields
	------
	seq: str
		the sequence (lowercase)
	header:
		the header of the sequence
	"""

	fl = open(filename, "r")
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


def iter_new_seqs(con, dbidVal, check_exists=True):
	"""
	iterate all new sequences in NewSequencesTable
	input:
	con: the database connection
	dbidVal: int
		the whole seq database id (from WholeSeqDatabaseTable)
	check_exists: bool, optional
		True to skip sequences already in the WholeSeqIDsTable for this whole seq database

	output:
	seq - the sequence
	dbbactid - the dbbact sequence id
	"""
	# a named cursor is server-side, so the rows are streamed instead of all loaded into memory
	cur2 = con.cursor(name='new_seqs')
	cur2.itersize = 50000
	if check_exists:
		cur2.execute('SELECT n.sequence, n.dbbactid FROM NewSequencesTable n WHERE NOT EXISTS '
					'(SELECT 1 FROM WholeSeqIDsTable w WHERE w.dbid=%s AND w.dbbactid=n.dbbactid::integer)', [dbidVal])
	else:
		cur2.execute('SELECT sequence, dbbactid FROM NewSequencesTable')
	for cseq, cid in cur2:
		yield(cseq, cid)
	cur2.close()


def hash_sequences(con, dbidVal, short_len=100, check_exists=True):
	'''hash all the sequences in the NewSequencesTable

	Parameters
	----------
	con: database connection
	dbidVal: int
		the whole seq database id (from WholeSeqDatabaseTable)
	short_len: int, optional
		the minimal sequence length in the database
	check_exists: bool, optional
		True to skip sequences already in the WholeSeqIDsTable (assuming SILVA and dbBact sequences have not been updated)

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

	debug(2, 'Scanning dbbact sequences')
	# if check_exists=True, sequences already in WholeSeqIDsTable are skipped by the query
	for cseq, cid in iter_new_seqs(con, dbidVal=dbidVal, check_exists=check_exists):
		total_seqs += 1
		clen = len(cseq)
		if clen < short_len:
			num_too_short += 1
			continue
		all_ids.add(cid)
		short_seq = cseq[:short_len]
		short_hash[short_seq][cseq] = cid
		if clen not in seq_lens:
			seq_lens.add(clen)
		seq_hash[cseq] = cid

	debug(2, 'processed %d dbbact sequences not in WholeSeqIDsTable. found %d new sequences' % (total_seqs, len(seq_hash)))
	debug(2, 'lens: %s' % seq_lens)
	debug(2, 'num too short: %d' % num_too_short)
	return all_ids, seq_hash, seq_lens, short_hash


def load_matches(cur, matches_file, all_ids):
	'''Bulk load the scan results into the temp tables tmp_matches and tmp_ids (dropped on commit)

	Parameters
	----------
	cur
	matches_file: file
		csv file with one line per match: dbbact_id,whole_seq_id
	all_ids: set of str
		all the dbBact sequence ids that were processed
	'''
	cur.execute('CREATE TEMP TABLE tmp_matches (dbbactid integer, wholeseqid text) ON COMMIT DROP')
	matches_file.seek(0)
	cur.copy_expert('COPY tmp_matches FROM STDIN WITH (FORMAT csv)', matches_file)
	cur.execute('CREATE TEMP TABLE tmp_ids (dbbactid integer) ON COMMIT DROP')
	cur.copy_expert('COPY tmp_ids FROM STDIN', io.StringIO(''.join('%s\n' % cid for cid in all_ids)))
	# temp tables are not auto-analyzed, and we need the statistics for good query plans
	cur.execute('ANALYZE tmp_matches')
	cur.execute('ANALYZE tmp_ids')


def update_whole_seq_ids_table(cur, whole_seq_db_id):
	'''Add the matches in tmp_matches to the WholeSeqIDsTable, and a 'na' entry for each dbBact id in tmp_ids without a match

	Parameters
	----------
	cur
	whole_seq_db_id: int
		the whole seq database id (from WhoeSeqDatabaseTable) - i.e. 1 for silva 13.2
	'''
	cur.execute('INSERT INTO WholeSeqIDsTable (dbid, dbbactid, wholeseqid) '
				'SELECT %s, dbbactid, wholeseqid FROM (SELECT DISTINCT dbbactid, wholeseqid FROM tmp_matches) m', [whole_seq_db_id])
	debug(2, 'added %d whole seq ids' % cur.rowcount)
	# add an entry with wholeseqid = 'na' for all dbbact sequences not matching any WholeSeqID, so we won't scan them next time
	cur.execute("INSERT INTO WholeSeqIDsTable (dbid, dbbactid, wholeseqid) "
				"SELECT %s, i.dbbactid, 'na' FROM tmp_ids i WHERE NOT EXISTS (SELECT 1 FROM tmp_matches m WHERE m.dbbactid=i.dbbactid)", [whole_seq_db_id])
	debug(2, 'added "na" for %d non-matched dbbact sequences' % cur.rowcount)


def update_sequencestosequences_table(cur):
	'''Update the SequenceToSequences table, that contains for each whole_seq region sequence the list of matching dbBact ids
	The table is used for fast sequence lookup when searching for matching dbBact sequences from other regions

	Parameters
	----------
	cur
		the tmp_matches temp table (dbbactid, wholeseqid) should already be loaded (using load_matches())
	'''
	# for each region sequence (from SequenceIDsTable) of the matched whole seq ids, get the list of new matching dbBact ids
	cur.execute('CREATE TEMP TABLE tmp_s2s ON COMMIT DROP AS '
				'SELECT s.sequence, array_agg(DISTINCT m.dbbactid::text) AS ids '
				'FROM (SELECT DISTINCT dbbactid, wholeseqid FROM tmp_matches) m JOIN SequenceIDsTable s ON s.wholeseqid=m.wholeseqid '
				'GROUP BY s.sequence')
	debug(2, 'found %d region sequences matching the new dbbact sequences' % cur.rowcount)
	cur.execute('ANALYZE tmp_s2s')
	# add the new ids to sequences already in the table (only rows which have at least one new id)
	cur.execute("UPDATE SequenceToSequenceTable t "
				"SET dbbactIDs=(SELECT string_agg(DISTINCT x, ',') FROM unnest(string_to_array(t.dbbactIDs, ',') || n.ids) AS x) "
				"FROM tmp_s2s n WHERE t.sequence=n.sequence AND NOT (n.ids <@ string_to_array(t.dbbactIDs, ','))")
	debug(2, 'updated %d existing SequenceToSequenceTable rows' % cur.rowcount)
	# and create the sequences not in the table
	cur.execute("INSERT INTO SequenceToSequenceTable (sequence, dbbactIDs) "
				"SELECT n.sequence, array_to_string(n.ids, ',') FROM tmp_s2s n "
				"WHERE NOT EXISTS (SELECT 1 FROM SequenceToSequenceTable t WHERE t.sequence=n.sequence)")
	debug(2, 'added %d new SequenceToSequenceTable rows' % cur.rowcount)


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
	count_matches = 0

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
	all_ids, seq_hash, seq_lens, short_hash = hash_sequences(con, dbidVal=whole_seq_dbid, short_len=150, check_exists=check_exists)
	if len(all_ids) == 0:
		debug(2, "no sequences to process.")
		return

	debug(2, 'Getting whole seq db IDs for %d dbbact sequences' % len(all_ids))
	# the matches are written to a temp file and bulk loaded after the scan (much faster than updating the database for each match)
	matches_file = tempfile.TemporaryFile(mode='w+', newline='')
	matches_writer = csv.writer(matches_file)
	# iterate over the silva database file and look for matches to any dbbact sequence
	idx = 0
	for cseq, chead in iter_fasta_seqs(whole_seq_fasta_name):
		isFound = False
		idx += 1
		if idx % 1000 == 1:
			debug(2, "count: %d" % idx)

		for cpos in range(len(cseq) - short_len + 1):
			ccseq = cseq[cpos:cpos + short_len]
			if ccseq in short_hash:
				# check all dbbact sequences starting with this prefix (i.e. 150bp and 200bp versions of the same sequence)
				for k, v in short_hash[ccseq].items():
					if cseq.startswith(k, cpos):
						# we have an exact match!
						# lets prepare the id string (sometimes has format 'ID.START.END TAXONOMY' we need to remove)
						cid = chead.split(' ')[0]
						split_cid = cid.split('.')
						if len(split_cid) > 2:
							cid = ".".join(split_cid[:-2])
						else:
							cid = ".".join(split_cid)
						cid = cid.lower()
						matches_writer.writerow([v, cid])
						count_matches += 1
						isFound = True
		if isFound:
			count_success += 1

	debug(2, 'found %d matches in %d whole seq sequences' % (count_matches, count_success))
	debug(2, 'loading matches into database')
	load_matches(cur, matches_file, all_ids)
	matches_file.close()
	debug(2, 'updating WholeSeqIDsTable')
	update_whole_seq_ids_table(cur, whole_seq_db_id=whole_seq_dbid)
	debug(2, 'updating SequenceToSequenceTable')
	update_sequencestosequences_table(cur)
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
