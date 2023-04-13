from collections import defaultdict
import psycopg2
import requests

from . import dbprimers
from .utils import debug
from . import dbannotations

# length for the seed sequence
# used for fast searching of sub sequences
SEED_SEQ_LEN = 100


def AddSequences(con, cur, sequences, taxonomies=None, ggids=None, primer='V4', commit=True, seq_translate_api=None):
    """
    Add sequence entries to database if they do not exist yet
    input:
    con,cur : database connection and cursor
    sequences: list of str
        the sequences to add
    taxonomies: list of str (optional)
        taxonomy of each sequence or None to add NA
    ggids: list of int (optional)
        list of GreenGenes id for each sequence or None to add 0
    primer: str (optional)
        Name of the primer (from PrimersTable). default is V4
    commit : bool (optional)
        True (default) to commit, False to wait with the commit

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    seqids : list of int or None
        list of the new ids or None if error enountered
    """
    # get the primer region id
    seqids = []
    numadded = 0
    idprimer = dbprimers.GetIdFromName(con, cur, primer)
    if idprimer < 0:
        debug(2, 'primer %s not found' % primer)
        return "primer %s not found" % primer, None
    debug(1, 'primerid %s' % idprimer)

    # prepare the queries for fast performance
    err = dbannotations._prepare_queries(con, cur)

    try:
        seqs_to_add_to_translator = {}
        for idx, cseq in enumerate(sequences):
            if len(cseq) < SEED_SEQ_LEN:
                errmsg = 'sequence too short (<%d) for sequence %s' % (SEED_SEQ_LEN, cseq)
                debug(4, errmsg)
                return errmsg, None
            # test if already exists, skip it. NOTE: we do not want the sequence translator sequences as result (we look for our sequence)
            err, cseqid = GetSequenceId(con, cur, sequence=cseq, idprimer=idprimer, no_shorter=True, no_longer=True, seq_translate_api=None)
            # if we get primer mismatch error, it means the sequence is in the database but with different primers
            if err == 'primer mismatch':
                return 'primer mismatch: The sequence in dbBact has a different primer than the requested primer (%s)\nfor sequence %s\nPlease contact dbBact support.' % (primer, cseq), None

            if len(cseqid) == 0:
                # not found, so need to add this sequence
                if taxonomies is None:
                    ctax = 'na'
                else:
                    ctax = taxonomies[idx].lower()
                if ggids is None:
                    cggid = 0
                else:
                    cggid = ggids[idx]
                cseq = cseq.lower()
                cseedseq = cseq[:SEED_SEQ_LEN]
                cur.execute('INSERT INTO SequencesTable (idPrimer,sequence,length,taxonomy,ggid,seedsequence) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id', [idprimer, cseq, len(cseq), ctax, cggid, cseedseq])
                cseqid = cur.fetchone()
                numadded += 1
                seqs_to_add_to_translator[cseqid[0]] = cseq
            if len(cseqid) > 1:
                msg = 'AddSequences - Same sequence appears twice in database: %s' % cseq
                debug(8, msg)
                return msg, None
            seqids.append(cseqid[0])

        if seq_translate_api is not None:
            debug(1, 'adding sequence to sequence translator queue')
            res = requests.post(seq_translate_api + '/add_sequences_to_queue', json={'seq_info': seqs_to_add_to_translator})
            if not res.ok:
                msg = 'add new sequences to sequence translator failed. error: %s' % res.content
                debug(7, msg)
                return msg, None
        else:
            debug(5, 'sequence translator not activated for add_sequence')

        if commit:
            con.commit()
        debug(3, "Added %d sequences (out of %d)" % (numadded, len(sequences)))
        return "", seqids

    except psycopg2.DatabaseError as e:
        debug(7, 'database error %s' % e)
        return "database error %s" % e, None


def SeqFromID(con, cur, seqids):
    '''Get the information about the sequence.
    Get the sequence (ACGT) and taxonomy from sequence id or list of sequence ids

    Parameters
    ----------
    con, cur
    seqids : int or list of int
        the ids to get the sequences for

    Returns
    -------
    sequences : list of dict (one per sequence). contains:
        'seq' : str (ACGT)
            the sequence
        'taxonomy' : str
            the taxonomy of the sequence or '' if unknown
        'total_annotations': int
            the number of annotations which this sequence is associated with
        'total_experiments': int
            the total number of experiments which this sequence is associated with
    '''
    if isinstance(seqids, int):
        seqids = [seqids]
    sequences = []
    for cseqid in seqids:
        cur.execute("SELECT sequence,coalesce(taxdomain,''),coalesce(taxphylum,''),  coalesce(taxclass,''),coalesce(taxorder,''),coalesce(taxfamily,''), coalesce(taxgenus,'') as taxonomy_str, total_annotations, total_experiments FROM SequencesTable WHERE id=%s", [cseqid])

        if cur.rowcount == 0:
            sequences.append({'seq': ''})
            continue
        res = cur.fetchone()
        firstTax = True
        taxStr = ''
        list_of_pre_str = ["d__", "p__", "c__", "o__", "f__", "g__"]
        for idx, val in enumerate(list_of_pre_str):
            if res[idx + 1]:
                if firstTax is False:
                    taxStr += ';'
                taxStr += val + res[idx + 1]
                firstTax = False

        cseqinfo = {'seq': res[0], 'taxonomy': taxStr, 'seqid': cseqid, 'total_annotations': res['total_annotations'], 'total_experiments': res['total_experiments']}
        sequences.append(cseqinfo)
    return '', sequences


def OBSOLETE_GetSequencesIds(con, cur, sequences, no_shorter=False, no_longer=False, seq_translate_api=None, dbname=None):
    """
    Get sequence ids for a sequence or list of sequences

    input:
    con,cur : database connection and cursor
    sequences : list of str (ACGT sequences)
    no_shorter : bool (optional)
        False (default) to enable shorter db sequences matching sequence, True to require at least length of query sequence
    no_longer : bool (optional)
        False (default) to enable longer db sequences matching sequence, True to require at least length of database sequence
    dbname: str or None, optional
        if None, assume sequences are acgt sequences
        if str, assume sequences are database ids and this is the database name (i.e. 'FJ978486' for 'silva', etc.)

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    ids : list of [list of int]
        the list of ids for each sequence (empty [] for sequences which were not found)
    """
    # prepare the queries for faster performance
    err = dbannotations._prepare_queries(con, cur)
    if isinstance(sequences, str):
        sequences = [sequences]
    ids = []
    for cseq in sequences:
        err, cid = GetSequenceId(con, cur, cseq, no_shorter=no_shorter, no_longer=no_longer, seq_translate_api=seq_translate_api, dbname=dbname)
        if err:
            # or should we skip?????
            return err, []
        ids.append(cid)
    return "", ids


def GetSequenceIdFromGG(con, cur, ggid):
    '''
    Get the sequence id for a given greengenes id (from rep. set 97%)

    Parameters
    ----------
    con,cur : database connection and cursor
    ggid : int
        The greengenes (rep_set 97%) identifier of the sequence

    Returns
    -------
    errmsg : str
        "" if ok, error msg if error encountered
    sid : list of int
        the ids of the matching sequences (empty tuple if not found)
        Note: can be more than one as several dbbact sequences can map to same ggid
    '''
    sid = []

    debug(1, 'get id for ggid %d' % ggid)
    cur.execute('SELECT id FROM SequencesTable WHERE ggid=%s', [ggid])
    if cur.rowcount == 0:
        errmsg = 'ggid %s not found in database' % ggid
        debug(1, errmsg)
        return errmsg, sid

    res = cur.fetchall()
    for cres in res:
        resid = cres[0]
        sid.append(resid)

    debug(1, 'found %d sequences for ggid %d' % (len(sid), ggid))
    return '', sid


def GetSequencesIds(con, cur, sequences, idprimer=None, no_shorter=False, no_longer=False, seq_translate_api=None, dbname=None):
    """
    Get sequence ids for a list of sequences

    input:
    con,cur : database connection and cursor
    sequences : list of [str (ACGT sequences or SILVA ID) or integers].
        If integer, interpret as greengenesID. Otherwise, if dbname is None interpret as acgt sequence. Otherwise it is a SILVA ID
    idprimer : int (optional)
        if supplied, verify the sequence is from this idPrimer
    no_shorter : bool (optional)
        False (default) to enable shorter db sequences matching sequence, True to require at least length of query sequence
    no_longer : bool (optional)
        False (default) to enable longer db sequences matching sequence, True to require at least length of database sequence
    seq_translate_api: str or None, optional
        str: the address of the sequence translator rest-api (default 127.0.0.1:5021). If supplied, will also return matching sequences on other regions based on SILVA/GG
        None: get only exact matches
    dbname: str or None, optional
        if None, assume sequences are acgt sequences
        if str, assume sequences are database ids and this is the database name (i.e. 'FJ978486' for 'silva', etc.)

    output:
    errmsg : str
        "" if ok, error msg if error encountered
        in case sequence matches but primer does not, error is "primer mismatch"
    sids : list of [list of int]
        the ids of the matching sequences (empty tuple if not found)
        Note: can be more than one as we also look for short subsequences / long supersequences
    """
    err = dbannotations._prepare_queries(con, cur)

    sids = []
    # get the sequence ids without region translation
    # we do one call to sequence translator to make it faster
    for cseq in sequences:
        err, cids = GetSequenceId(con, cur, cseq, idprimer=idprimer, no_shorter=no_shorter, no_longer=no_longer, seq_translate_api=None, dbname=dbname)
        sids.append(list(cids))

    # and now call the sequence translator for all sequences
    if seq_translate_api is not None:
        if dbname is None:
            debug(2, 'translating sequence to other regions')
            res = requests.post(seq_translate_api + '/get_ids_for_seqs', json={'sequences': sequences})
        else:
            debug(2, 'getting dbids from wholeseq ids')
            res = requests.post(seq_translate_api + '/get_dbbact_ids_from_wholeseq_ids', json={'whole_seq_ids': sequences, 'dbname': dbname})
        if res.ok:
            trans_ids = res.json()['dbbact_ids']
        else:
            debug(5, 'got error from sequence translator: %s' % res.content)
            trans_ids = []
        # and merge the ids
        for cidx in range(len(sequences)):
            sids[cidx].extend(trans_ids[cidx])
    else:
        debug(2, 'not translating')
    return '', sids


def GetSequenceId(con, cur, sequence, idprimer=None, no_shorter=False, no_longer=False, seq_translate_api=None, dbname=None):
    """
    Get sequence ids for a sequence

    input:
    con,cur : database connection and cursor
    sequence : str (ACGT sequences or SILVA ID) or integer.
        If integer, interpret as greengenesID. Otherwise, if dbname is None interpret as acgt sequence. Otherwise it is a SILVA ID
    idprimer : int (optional)
        if supplied, verify the sequence is from this idPrimer
    no_shorter : bool (optional)
        False (default) to enable shorter db sequences matching sequence, True to require at least length of query sequence
    no_longer : bool (optional)
        False (default) to enable longer db sequences matching sequence, True to require at least length of database sequence
    seq_translate_api: str or None, optional
        str: the address of the sequence translator rest-api (default 127.0.0.1:5021). If supplied, will also return matching sequences on other regions based on SILVA/GG
        None: get only exact matches
    dbname: str or None, optional
        if None, assume sequences are acgt sequences
        if str, assume sequences are database ids and this is the database name (i.e. 'FJ978486' for 'silva', etc.)

    output:
    errmsg : str
        "" if ok, error msg if error encountered
        in case sequence matches but primer does not, error is "primer mismatch"
    sid : list of int
        the ids of the matching sequences (empty tuple if not found)
        Note: can be more than one as we also look for short subsequences / long supersequences
    """
    # check if the sequence is made only of digits assume it is a greengenes id
    if sequence.isdigit():
        debug(1, 'getting id for ggid %s' % sequence)
        return GetSequenceIdFromGG(con, cur, int(sequence))

    sid = []
    cseq = sequence.lower()
    # if looking for dbbact sequence matches
    if dbname is None:
        if len(cseq) < SEED_SEQ_LEN:
            errmsg = 'sequence too short (<%d) for sequence %s' % (SEED_SEQ_LEN, cseq)
            debug(4, errmsg)
            return errmsg, sid

        # if looking for exact sequence, look up fast using exact match
        if no_shorter and no_longer:
            debug(2, 'noshortnolong')
            err = dbannotations._prepare_queries(con, cur)
            if err:
                return err, []
            # cur.execute('SELECT id, idprimer FROM SequencesTable WHERE sequence=%s LIMIT 1', [cseq])
            cur.execute('EXECUTE get_sequence_id_exact(%s)', [cseq])
            if cur.rowcount > 0:
                res = cur.fetchone()
                if idprimer is not None:
                    if res['idprimer'] != idprimer:
                        debug(8, 'Matching sequence %s but non-matching primer %d (query primer was %d)' % (sequence, res['idprimer'], idprimer))
                        return 'primer mismatch', []
                sid = [res['id']]
        else:
            # look for all sequences matching the seed
            cseedseq = cseq[:SEED_SEQ_LEN]
            # cur.execute('SELECT id,sequence FROM SequencesTable WHERE seedsequence=%s', [cseedseq])
            cur.execute('EXECUTE get_sequence_id_seed(%s)', [cseedseq])
            cseqlen = len(cseq)
            res = cur.fetchall()
            found_seq = False
            for cres in res:
                resid = cres[0]
                resseq = cres[1]
                if no_shorter:
                    if len(resseq) < cseqlen:
                        continue
                    comparelen = cseqlen
                else:
                    comparelen = min(len(resseq), cseqlen)
                if no_longer:
                    if len(resseq) > cseqlen:
                        continue
                if cseq[:comparelen] == resseq[:comparelen]:
                    found_seq = True
                    if idprimer is None:
                        sid.append(resid)
                    else:
                        cur.execute('EXECUTE get_sequence_primer(%s)', [resid])
                        # cur.execute('SELECT idPrimer FROM SequencesTable WHERE id=%s LIMIT 1', [resid])
                        res = cur.fetchone()
                        if res[0] == idprimer:
                            sid.append(resid)
            # if we found the sequence but not
            if found_seq and len(sid) == 0:
                return 'primer mismatch', []

    if seq_translate_api is not None:
        if dbname is None:
            debug(2, 'translating sequence to other regions')
            res = requests.post(seq_translate_api + '/get_ids_for_seqs', json={'sequences': [sequence]})
        else:
            debug(2, 'getting dbids from wholeseq ids')
            res = requests.post(seq_translate_api + '/get_dbbact_ids_from_wholeseq_ids', json={'whole_seq_ids': [sequence], 'dbname': dbname})
        if res.ok:
            trans_ids = res.json()['dbbact_ids'][0]
        else:
            debug(5, 'got error from sequence translator: %s' % res.content)
            trans_ids = []
        sid.extend(trans_ids)
    else:
        debug(2, 'not translating')

    if len(sid) == 0:
        errmsg = 'sequence %s not found' % sequence
        debug(1, errmsg)
        return errmsg, sid

    sid = list(set(sid))
    return '', sid

    # # if no regionid specified, fetch only 1 (faster)
    # if idprimer is None:
    #   cur.execute('SELECT id FROM SequencesTable WHERE sequence=%s LIMIT 1',[cseq])
    #   if cur.rowcount==0:
    #       sid=-1
    #       debug(2,'sequence %s not found' % sequence)
    #   else:
    #       sid=cur.fetchone()[0]
    #       debug(1,'sequence %s found id %d' % (sequence,sid))
    #   return "",sid
    # # regionid was specified, so test all matching sequences
    # cur.execute('SELECT id,idPrimer FROM SequencesTable WHERE sequence=%s LIMIT 1',[cseq])
    # for cres in cur:
    #   if cres[1]==idprimer:
    #       sid=cres[0]
    #       debug(1,'sequence %s found with primer. id %d' % (sequence,sid))
    #       return "",sid
    # debug(2,'sequence %s not found with primer. non primer matches: %d' % (sequence,cur.rowcount))
    # return 'sequence %s not found with primer. non primer matches: %d' % (sequence,cur.rowcount),-1


def get_taxonomy_seqids(con, cur, taxonomy, userid=None):
    '''Get a list of all dbbact sequences containing the taxonomy as substring of the dbbact taxonomy

    Parameters
    ----------
    con,cur
    taxonomy : str
        the taxonomy substring to look for
    userid : int (optional)
        the userid of the querying user (to enable searching private annotations)

    Returns
    -------
    list of int
        The sequenceids for all sequences containing the taxonomy
    '''
    taxonomy = taxonomy.lower()
    taxStr = taxonomy
    debug(1, 'GetTaxonomyAnnotationIDS for taxonomy %s' % taxonomy)
    cur.execute('SELECT id from SequencesTable where (taxrootrank ILIKE %s OR taxdomain ILIKE %s OR taxphylum ILIKE %s OR taxclass ILIKE %s OR taxfamily ILIKE %s OR taxgenus ILIKE %s OR taxorder ILIKE %s)', [taxStr, taxStr, taxStr, taxStr, taxStr, taxStr, taxStr])
    res = cur.fetchall()
    seqids = []
    for cres in res:
        seqids.append(cres[0])
    debug(1, 'found %d matching sequences for the taxonomy' % len(seqids))
    return seqids


def GetTaxonomyAnnotationIDs(con, cur, taxonomy, userid=None):
    '''
    Get annotationids for all annotations containing any sequence matching the taxonomy (substring)

    Parameters
    ----------
    con,cur
    taxonomy : str
        the taxonomy substring to look for
    userid : int (optional)
        the userid of the querying user (to enable searching private annotations)

    Returns
    -------
    annotationids : list of (int, int) (annotationid, count)
        list containing the ids of all annotations that contain a sequence with the taxonomy and the count of number of sequences from the taxonomy in that annotation
    seqids : list of int
        list of the sequenceids that have this annotation
    '''
    seqids = get_taxonomy_seqids(con, cur, taxonomy=taxonomy, userid=userid)
    # taxonomy = taxonomy.lower()
    # taxStr = taxonomy
    # debug(1, 'GetTaxonomyAnnotationIDS for taxonomy %s' % taxonomy)
    # cur.execute('SELECT id from SequencesTable where (taxrootrank ILIKE %s OR taxdomain ILIKE %s OR taxphylum ILIKE %s OR taxclass ILIKE %s OR taxfamily ILIKE %s OR taxgenus ILIKE %s OR taxorder ILIKE %s)', [taxStr, taxStr, taxStr, taxStr, taxStr, taxStr, taxStr])
    # res = cur.fetchall()
    # seqids = []
    # for cres in res:
    #     seqids.append(cres[0])
    # debug(1, 'found %d matching sequences for the taxonomy' % len(seqids))
    annotationids_dict = defaultdict(int)
    for cseq in seqids:
        cur.execute('SELECT annotationid from sequencesAnnotationTable where seqid=%s', [cseq])
        res = cur.fetchall()
        for cres in res:
            annotationids_dict[cres[0]] += 1
    # NOTE: need to add user validation for the ids!!!!!!
    debug(1, 'found %d unique annotations for the taxonomy' % len(annotationids_dict))
    annotationids = []
    for k, v in annotationids_dict.items():
        annotationids.append((k, v))
    return '', annotationids, seqids


def GetTaxonomyAnnotations(con, cur, taxonomy, userid=None):
    '''
    Get annotations for all annotations containing any sequence matching the taxonomy (substring)

    Parameters
    ----------
    con,cur
    taxonomy : str
        the taxonomy substring to look for
    userid : int (optional)
        the userid of the querying user (to enable searching private annotations)

    Returns
    -------
    annotations : list of tuples (annotation, counts)
        list containing the details for all annotations that contain a sequence with the taxonomy
        annotation - (see dbannotations.GetAnnotationsFromID() )
        counts - the number of sequences from taxonomy appearing in this annotations
    seqids : list of int
        list of the sequenceids which have this taxonomy
    '''
    debug(1, 'GetTaxonomyAnnotations for taxonomy %s' % taxonomy)
    dbannotations._prepare_queries(con, cur)
    # get the annotation ids
    err, annotationids, seqids = GetTaxonomyAnnotationIDs(con, cur, taxonomy, userid)
    if err:
        errmsg = 'Failed to get annotationIDs for taxonomy %s: %s' % (taxonomy, err)
        debug(6, errmsg)
        return errmsg, None
    # and get the annotation details for each
    annotations = []
    for cres in annotationids:
        cid = cres[0]
        ccount = cres[1]
        err, cdetails = dbannotations.GetAnnotationsFromID(con, cur, cid)
        if err:
            debug(6, err)
            continue
        annotations.append((cdetails, ccount))
    debug(1, 'got %d details' % len(annotations))
    return '', annotations, seqids


def GetHashAnnotationIDs(con, cur, hash_str, userid=None):
    '''
    Get annotationids for all annotations containing any sequence matching the Hash (substring)

    Parameters
    ----------
    con,cur
    Hash : str
        the Hash substring to look for
    userid : int (optional)
        the userid of the querying user (to enable searching private annotations)

    Returns
    -------
    annotationids : list of (int, int) (annotationid, count)
        list containing the ids of all annotations that contain a sequence with the Hash and the count of number of sequences from the Hash in that annotation
    seqids : list of int
        list of the sequenceids that have this annotation
    '''
    hash_str = hash_str.lower()
    taxStr = hash_str
    debug(1, 'GetHashAnnotationIDS for Hash %s' % hash_str)
    cur.execute('SELECT id,sequence from SequencesTable where (hashfull ILIKE %s or hash150 ILIKE %s or hash100 ILIKE %s)', [hash_str, hash_str, hash_str])
    res = cur.fetchall()
    seqids = []
    seqnames = []
    for cres in res:
        seqids.append(cres[0])
        seqnames.append(cres[1])
    debug(1, 'found %d matching sequences for the Hash' % len(seqids))
    annotationids_dict = defaultdict(int)
    for cseq in seqids:
        cur.execute('SELECT annotationid from sequencesAnnotationTable where seqid=%s', [cseq])
        res = cur.fetchall()
        for cres in res:
            annotationids_dict[cres[0]] += 1
    # NOTE: need to add user validation for the ids!!!!!!
    debug(1, 'found %d unique annotations for the Hash' % len(annotationids_dict))
    annotationids = []
    for k, v in annotationids_dict.items():
        annotationids.append((k, v))
    return '', annotationids, seqids, seqnames


# superceded by dbbact_sequence_translator rest API
# def get_seqs_from_db_id(con, cur, db_name, db_seq_id):
#     '''Get all sequences that match the db_seq_id supplied for silva/greengenes

#     Parameters
#     ----------
#     con, cur
#     db_name: str
#         name of the database from which the id originates. can be "silva" or "gg"
#     db_seq_id: str
#         the sequence identifier in the database (i.e. 'FJ978486.1.1387' for silva or '1111883' for greengenes)

#     Returns
#     -------
#     error: str or '' if ok
#     list of int
#         the dbbact ids for all the dbbact sequences matching the db_seq_id
#     list of str
#         the actual sequences for the dbbact sequences matching the db_seq_id (same order)
#     '''
#     database_ids = {'silva': 1, 'gg': 2}
#     if db_name in database_ids:
#         db_id = database_ids[db_name]
#     else:
#         err = 'database id %s not found. options are: %s' % database_ids.keys()
#         debug(9, err)
#         return err, [], []
#     db_seq_id = db_seq_id.lower()
#     cur.execute("SELECT id,sequence FROM SequencesTable where id in (select distinct dbbactid from WholeSeqIDsTable where WholeSeqID=%s AND dbid=%s)", [db_seq_id, db_id])
#     seq_ids = []
#     sequences = []
#     res = cur.fetchall()
#     for cres in res:
#         seq_ids.append(cres[0])
#         sequences.append(cres[1])
#     debug(1, 'found %d dbbact sequences for seqid %s' % (len(seq_ids), db_seq_id))
#     return '', seq_ids, sequences


# superceded by dbbact_sequence_translator REST-API and the dbbact parameter in GetSequenceAnnotations() etc.
# def GetGgAnnotationIDs(con, cur, gg_str, userid=None):
#     '''
#     Get annotationids for all annotations containing any sequence matching the gg id (substring)

#     Parameters
#     ----------
#     con,cur
#     gg : str
#         the gg id substring to look for
#     userid : int (optional)
#         the userid of the querying user (to enable searching private annotations)

#     Returns
#     -------
#     annotationids : list of (int, int) (annotationid, count)
#         list containing the ids of all annotations that contain a sequence with the Hash and the count of number of sequences from the Hash in that annotation
#     seqids : list of int
#         list of the sequenceids that have this annotation
#     '''
#     gg_str = gg_str.lower()
#     ggStr = gg_str
#     debug(1, 'GetGgAnnotationIDs for gg %s' % gg_str)

#     err, seqids, seqnames = get_seqs_from_db_id(con, cur, 'gg', ggStr)
#     if err != '':
#         return err, [], [], []

#     annotationids_dict = defaultdict(int)
#     for cseq in seqids:
#         cur.execute('SELECT annotationid from sequencesAnnotationTable where seqid=%s', [cseq])
#         res = cur.fetchall()
#         for cres in res:
#             annotationids_dict[cres[0]] += 1
#     # NOTE: need to add user validation for the ids!!!!!!
#     debug(1, 'found %d unique annotations for the gg' % len(annotationids_dict))
#     annotationids = []
#     for k, v in annotationids_dict.items():
#         annotationids.append((k, v))
#     return '', annotationids, seqids, seqnames


# def GetSilvaAnnotationIDs(con, cur, silva_str, userid=None):
#     '''
#     Get annotationids for all annotations containing any sequence matching the silva id (substring)

#     Parameters
#     ----------
#     con,cur
#     Silva : str
#         the silva substring to look for
#     userid : int (optional)
#         the userid of the querying user (to enable searching private annotations)

#     Returns
#     -------
#     err: str
#         the error encountered or '' if successful
#     annotationids : list of (int, int) (annotationid, count)
#         list containing the ids of all annotations that contain a sequence with the silvaID and the count of number of sequences with the silvaID in that annotation
#     seqids : list of int
#         list of the sequenceids that have this silvaID
#     seqnames: list of str
#         the sequences matching the silvaID
#     '''
#     debug(1, 'GetSilvaAnnotationIDs for Silva %s' % silva_str)

#     err, seqids, seqnames = get_seqs_from_db_id(con, cur, 'silva', silva_str)
#     if err != '':
#         return err, [], [], []
#     debug(1, 'found %d matching sequences for the silva' % len(seqids))
#     annotationids_dict = defaultdict(int)
#     for cseq in seqids:
#         cur.execute('SELECT annotationid from sequencesAnnotationTable where seqid=%s', [cseq])
#         res = cur.fetchall()
#         for cres in res:
#             annotationids_dict[cres[0]] += 1
#     # NOTE: need to add user validation for the ids!!!!!!
#     debug(1, 'found %d unique annotations for the Silva' % len(annotationids_dict))
#     annotationids = []
#     for k, v in annotationids_dict.items():
#         annotationids.append((k, v))
#     return '', annotationids, seqids, seqnames


def GetHashAnnotations(con, cur, hash_str, userid=None):
    '''
    Get annotations for all annotations containing any sequence matching the hash (substring)

    Parameters
    ----------
    con,cur
    taxonomy : str
        the hash substring to look for
    userid : int (optional)
        the userid of the querying user (to enable searching private annotations)

    Returns
    -------
    annotations : list of tuples (annotation, counts)
        list containing the details for all annotations that contain a sequence with the taxonomy
        annotation - (see dbannotations.GetAnnotationsFromID() )
        counts - the number of sequences from taxonomy appearing in this annotations
    seqids : list of int
        list of the sequenceids which have this taxonomy
    seqnames : list of sequence strings
    '''
    debug(1, 'GetHashAnnotations for hash %s' % hash_str)
    # get the annotation ids
    err, annotationids, seqids, seqnames = GetHashAnnotationIDs(con, cur, hash_str, userid)
    if err:
        errmsg = 'Failed to get annotationIDs for hash_str %s: %s' % (hash_str, err)
        debug(6, errmsg)
        return errmsg, None
    # and get the annotation details for each
    annotations = []
    for cres in annotationids:
        cid = cres[0]
        ccount = cres[1]
        err, cdetails = dbannotations.GetAnnotationsFromID(con, cur, cid)
        if err:
            debug(6, err)
            continue
        annotations.append((cdetails, ccount))
    debug(1, 'got %d details' % len(annotations))
    return '', annotations, seqids, seqnames


# superceded by dbname parameter in GetSequenceAnnotations() etc.
# def GetGgAnnotations(con, cur, gg_str, userid=None):
#     '''
#     Get annotations for all annotations containing any sequence matching the hash (substring)

#     Parameters
#     ----------
#     con,cur
#     gg_str : str
#         the gg id substring to look for
#     userid : int (optional)
#         the userid of the querying user (to enable searching private annotations)

#     Returns
#     -------
#     annotations : list of tuples (annotation, counts)
#         list containing the details for all annotations that contain a sequence with the taxonomy
#         annotation - (see dbannotations.GetAnnotationsFromID() )
#         counts - the number of sequences from taxonomy appearing in this annotations
#     seqids : list of int
#         list of the sequenceids which have this taxonomy
#     seqnames : list of sequence strings
#     '''
#     debug(1, 'GetGgAnnotations for hash %s' % gg_str)
#     # get the annotation ids
#     err, annotationids, seqids, seqnames = GetGgAnnotationIDs(con, cur, gg_str, userid)
#     if err:
#         errmsg = 'Failed to get annotationIDs for gg_str %s: %s' % (gg_str, err)
#         debug(6, errmsg)
#         return errmsg, None
#     # and get the annotation details for each
#     annotations = []
#     for cres in annotationids:
#         cid = cres[0]
#         ccount = cres[1]
#         err, cdetails = dbannotations.GetAnnotationsFromID(con, cur, cid)
#         if err:
#             debug(6, err)
#             continue
#         annotations.append((cdetails, ccount))
#     debug(1, 'got %d details' % len(annotations))
#     return '', annotations, seqids, seqnames


# def GetSilvaAnnotations(con, cur, silva_str, userid=None):
#     '''
#     Get annotations for all annotations containing any sequence matching the silvaID (substring)

#     Parameters
#     ----------
#     con,cur
#     silva_str : str
#         the silva id substring to look for
#     userid : int (optional)
#         the userid of the querying user (to enable searching private annotations)

#     Returns
#     -------
#     annotations : list of tuples (annotation, counts)
#         list containing the details for all annotations that contain a sequence with the taxonomy
#         annotation - (see dbannotations.GetAnnotationsFromID() )
#         counts - the number of sequences from taxonomy appearing in this annotations
#     seqids : list of int
#         list of the sequenceids which have this taxonomy
#     seqnames : list of sequence strings
#     '''
#     debug(1, 'GetSilvaAnnotations for silva ID %s' % silva_str)
#     # get the annotation ids
#     err, annotationids, seqids, seqnames = GetSilvaAnnotationIDs(con, cur, silva_str, userid)
#     if err:
#         errmsg = 'Failed to get annotationIDs for silva_str %s: %s' % (silva_str, err)
#         debug(6, errmsg)
#         return errmsg, None
#     # and get the annotation details for each
#     annotations = []
#     for cres in annotationids:
#         cid = cres[0]
#         ccount = cres[1]
#         err, cdetails = dbannotations.GetAnnotationsFromID(con, cur, cid)
#         if err:
#             debug(6, err)
#             continue
#         annotations.append((cdetails, ccount))
#     debug(1, 'got %d details' % len(annotations))
#     return '', annotations, seqids, seqnames


def GetSequenceWithNoTaxonomyID(con, cur):
    '''
    Get sequence with no taxonomy (if any)

    Parameters
    ----------
    con,cur

    Returns
    -------
    errmsg: '' if ok, otherwise the error message
    sequence id : a sequenceid of a sequence without taxonomy, or -1 if no such sequences exist
    '''
    debug(1, 'GetSequenceWithNoTaxonomy')

    try:
        cur.execute("select id from sequencestable where (COALESCE(taxrootrank,'')='' AND COALESCE(taxdomain,'')='' AND COALESCE(taxphylum,'')='' AND COALESCE(taxclass,'')='' AND COALESCE(taxfamily,'')='' AND COALESCE(taxgenus,'')='' AND COALESCE(taxorder,'')='') limit 1")
        if cur.rowcount == 0:
            debug(1, 'no sequences without taxonomy')
            return '', -1
        res = cur.fetchone()
        return_id = res[0]
        return '', return_id
    except Exception as e:
        errmsg = 'database error when fetching samples with no taxonomy: %s' % e
        debug(5, errmsg)
        return errmsg, -1


def GetSequenceWithNoHashID(con, cur):
    '''
    Get sequence with no hash value (if any)

    Parameters
    ----------
    con,cur

    Returns
    -------
    errmsg: '' if ok, otherwise the error message
    sequence id : a sequenceid of a sequence without hash, or -1 if no such sequences exist
    '''
    debug(1, 'GetSequenceWithNoHashID')

    try:
        cur.execute("select id from sequencestable where (COALESCE(hashfull,'')='' AND COALESCE(hash150,'')='' AND COALESCE(hash100,'')='') limit 1")
        if cur.rowcount == 0:
            debug(1, 'no sequences without hash found')
            return '', -1
        res = cur.fetchone()
        return_id = res[0]
        return '', return_id
    except Exception as e:
        errmsg = 'database error when fetching samples with no hashid: %s' % e
        debug(5, errmsg)
        return errmsg, -1


def GetSequenceStrByID(con, cur, seq_id):
    '''
    Get sequence from seqid

    Parameters
    ----------
    con,cur
    seq_id: int
        the dbbact seqid

    Returns
    -------
    sequence str : return the sequence str
    '''
    debug(1, 'GetSequenceStrByID')

    cur.execute("select sequence from sequencestable where id=%s" % seq_id)
    if cur.rowcount == 0:
        errmsg = 'no sequeence for seqid %s' % seq_id
        debug(1, errmsg)
        return errmsg, seq_id
    res = cur.fetchone()
    return_id = res[0]

    return '', return_id


def UpdateHash(con, cur, seq_id, hash_seq_full, hash_seq_150, hash_seq_100):
    '''
    update hash information

    Parameters
    ----------
    con,cur
    seq_id
    hash_seq_full - hash for full
    hash_seq_150 - hash for first 150 characters
    hash_seq_100 - hash for first 100 characters

    Returns
    -------
    true or false
    '''
    debug(1, 'UpdateHash')

    try:
        cur.execute("update sequencestable set hashfull='%s',hash150='%s',hash100='%s' where id=%s" % (hash_seq_full, hash_seq_150, hash_seq_100, seq_id))
        con.commit()
        return True
    except:
        return False


def AddSequenceTax(con, cur, seq_id, col, value):
    '''
    update taxonomy record value

    Parameters
    ----------
    con,cur
    seq_id
    col - taxonomyrank coloumn name
    value - taxonomyrank value

    Returns
    -------
    true or false
    '''
    debug(1, 'GetSequenceStrByID')

    try:
        cur.execute("update sequencestable set %s='%s' where id=%s" % (col, value, seq_id))
        con.commit()
        return True
    except:
        return False


def GetSequenceTaxonomy(con, cur, sequence, region=None, userid=0):
    """
    Get taxonomy str for given sequence

    Parameters
    ----------
    con,cur :
    sequence : str ('ACGT')
        the sequence to search for in the database
    region : int (optional)
        None to not compare region, or the regionid the sequence is from
    userid : int (optional)
        the id of the user requesting the annotations. Private annotations with non-matching user will not be returned

    Returns
    -------
    err : str
        The error encountered or '' if ok
    taxonomy: str
        The taxonomy string (of format d__XXX;p__YYYY;...)
    """

    debug(1, 'GetSequenceTaxonomy sequence %s' % sequence)

    cseq = sequence.lower()
    cur.execute("SELECT coalesce(taxdomain,''),coalesce(taxphylum,''),  coalesce(taxclass,''),coalesce(taxorder,''),coalesce(taxfamily,''), coalesce(taxgenus,'') as taxonomy_str FROM SequencesTable WHERE sequence=%s", [cseq])

    if cur.rowcount == 0:
        debug(1, 'taxonomy not found for sequence %s' % cseq)
        # ctaxinfo = {'taxonomy': 'NA'}
        # return '', ctaxinfo
        return '', 'NA'

    res = cur.fetchone()
    firstTax = True
    taxStr = ''
    list_of_pre_str = ["d__", "p__", "c__", "o__", "f__", "g__"]
    for idx, val in enumerate(list_of_pre_str):
        if res[idx]:
            if firstTax is False:
                taxStr += ';'
            taxStr += val + res[idx]
            firstTax = False

    # ctaxinfo = {'taxonomy': taxStr}
    # return '', ctaxinfo
    return '', taxStr


def get_sequences_primer(con, cur, sequences):
    '''Get the primer region id and name for the list of sequences (assumes all are from same region)
    results are based on the region assigned to the matching sequences in dbbact

    Parameters
    ----------
    con, cur:
    sequences: list of str
        the DNA sequences (acgt)

    Returns
    -------
    err: str
        the error encountered or empty string '' if ok
    primerid: int
        the primmer id
    primername: str
        name of the region (i.e. 'v4' etc.)
    '''
    debug(1, 'get_sequences_primer for %d sequences' % len(sequences))

    # prepare queries for faster running
    err = dbannotations._prepare_queries(con, cur)

    primerid = None
    for cseq in sequences:
        err, seqid = GetSequenceId(con, cur, sequence=cseq, no_shorter=False, no_longer=False, seq_translate_api=None)
        if len(seqid) != 1:
            continue
        cur.execute('SELECT idprimer FROM SequencesTable WHERE id=%s LIMIT 1', [seqid[0]])
        res = cur.fetchone()
        cpid = res['idprimer']
        if primerid is None:
            primerid = cpid
        if cpid != primerid:
            msg = 'encountered >1 primers (%d, %d) for sequences' % (primerid, cpid)
            debug(5, msg)
            return msg, 0, []
    if primerid is None:
        # 0 is the 'na' primer
        primerid = 0
        # return 'no sequences match in dbbact.', 0, ''
    err, primer_name = dbprimers.GetNameFromID(con, cur, primerid)
    if err:
        return err, 0, ''
    return '', primerid, primer_name


def get_whole_seq_taxonomy(con, cur, sequence, seq_translate_api):
    '''Get the whole sequence database taxonomies matching a given sequence
    uses the sequence_translator_api interface

    Parameters
    ----------
    con, cur
    sequence: str ('ACGT')
        the sequence to match
    seq_translate_api:
        the sequence_translator interface

    Returns
    -------
    species, names, fullnames, ids: list of str
        species - the species name (if available) or '' (i.e. [clostridium] clostridioforme 90a3)
        names - the highest resolution taxonomy name (i.e. bacteria;firmicutes;clostridia;clostridiales;lachnospiraceae;lachnoclostridium;uncultured bacterium)
        fullnames - the full taxonomy string (i.e.   'fj506124.1.1377 bacteria;firmicutes;clostridia;clostridiales;lachnospiraceae;lachnoclostridium;uncultured bacterium',)
        ids - the silva ids (i.e. fj506124)
    '''
    res = requests.post(seq_translate_api + '/get_whole_seq_taxonomy', json={'sequence': sequence})
    if not res.ok:
        msg = 'failed to get whole seq taxonomies for sequence. error: %s' % res.content
        debug(7, msg)
        return msg, [], [], [], []
    res = res.json()
    ids = res['ids']
    species = res['species']
    names = res['names']
    fullnames = res['fullnames']
    return '', species, names, fullnames, ids


def get_species_seqs(con, cur, species, seq_translate_api):
    ''' Get the sequences matching a given species name, using exact SILVA matches

    Parameters
    ----------
    con, cur
    species: str
        the species name to search for (i.e. 'akkermansia muciniphila')
    seq_translate_api:

    Returns
    -------
    ids: list of int
        dbBac sequence ids of matching sequences
    seqs: list of str
        the matching sequences
    '''
    debug(2, 'get_species_seqs for %s' % species)
    res = requests.post(seq_translate_api + '/get_species_seqs', json={'species': species})
    if not res.ok:
        msg = 'failed to get matching sequence translator sequences. error: %s' % res.content
        debug(3, msg)
        return msg, [], []

    ids = res.json()['ids']
    debug(2, 'found %d seqs' % len(ids))
    return '', ids, ids


def get_close_sequences(con, cur, sequence, max_mismatches=1):
    '''Get the sequences in dbbact that are close to the given sequence
    Search for sequences with up to max_mismatches mismatches to the given sequence
    
    Parameters
    ----------
    con, cur
    sequence: str ('ACGT')
        the sequence to search for close enough matches
    max_mismatches: int, optional
        the maximum number of mismatches allowed
    
    Returns
    -------
    err: str
        the error encountered or empty string '' if ok
    sequences: list of str
        the matching sequences
    seq_ids: list of int
        the matching sequences dbbact ids
    '''
    debug(1, 'get_close_sequences for sequence %s' % sequence)
    if max_mismatches > 5:
        return 'max_mismatches must be <= 5', [], []
    sequence = sequence.lower()
    sim_thresh = 1 - max_mismatches / len(sequence)
    debug(1, 'sim_thresh: %f' % sim_thresh)
    cur.execute('SET pg_trgm.similarity_threshold = %s', [sim_thresh])
    cur.execute('SELECT id, sequence FROM SequencesTable WHERE sequence %% %s', [sequence])
    res = cur.fetchall()
    if len(res) == 0:
        debug(1, 'no sequences found')
        return '', [], []
    debug(1, 'found %d sequences' % len(res))
    sequences = []
    seq_ids = []
    for cres in res:
        cseq = cres['sequence']
        # count the number of mismatches between cseq and sequence
        mismatches = 0
        for i in range(len(cseq)):
            if cseq[i] != sequence[i]:
                mismatches += 1
        if mismatches > max_mismatches:
            debug(1, 'sequence %s has %d mismatches, skipping' % (cseq, mismatches)
            continue
        sequences.append(cseq)
        seq_ids.append(cres['id'])
    return '', sequences, seq_ids
