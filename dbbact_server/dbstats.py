from .utils import debug


def GetStats(con, cur):
    """
    Get statistics about the database
    input:
    con,cur : database connection and cursor

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    stats : dict
        containing statistics about the database tables
        keys:
        'NumSequences': number of unique sequences in the database (all regions)
        'NumAnnotations': number of annotations in the database
        'NumSeqAnnotations': number of associatins in the database (sequence-annotation links)
        'NumExperiments': number of experiments in the database
        'Databse': name of the database for which the statistics are

    """

    # number of unique sequences
    debug(1, 'Get db stats')
    stats = {}
    # get number of sequences
    cur.execute("SELECT COUNT(*) from sequencestable")
    stats['NumSequences'] = cur.fetchone()[0]

    # get number of annotations
    cur.execute("SELECT COUNT(*) from annotationstable")
    stats['NumAnnotations'] = cur.fetchone()[0]

    # get number of sequence annotations
    cur.execute("SELECT COUNT(*) from sequencesannotationtable")
    stats['NumSeqAnnotations'] = cur.fetchone()[0]

    # get number of ontologies
    cur.execute("SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = 'ontologytable'")
    stats['NumOntologyTerms'] = cur.fetchone()[0]

    # get number of experiments
    cur.execute("SELECT expid from experimentsTable")
    res = cur.fetchall()
    explist = set()
    for cres in res:
        explist.add(cres[0])
    stats['NumExperiments'] = len(explist)

    cur.execute('SELECT current_database()')
    res = cur.fetchone()
    stats['Database'] = res[0]
    return '', stats


def GetStats_Fast(con, cur):
    """
    Get statistics about the database
    NOTE: this is approximate and does not work nicely. but supposed to be faster!

    input:
    con,cur : database connection and cursor

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    stats : json
        containing statistics about the database tables
    """

    # number of unique sequences
    debug(1, 'Get db stats')
    stats = {}
    # get number of sequences
    cur.execute("SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = 'sequencestable'")
    stats['NumSequences'] = cur.fetchone()[0]

    # get number of annotations
    cur.execute("SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = 'annotationstable'")
    stats['NumAnnotations'] = cur.fetchone()[0]

    # get number of sequence annotations
    cur.execute("SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = 'sequencesannotationtable'")
    stats['NumSeqAnnotations'] = cur.fetchone()[0]

    # get number of ontologies
    cur.execute("SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = 'ontologytable'")
    stats['NumOntologyTerms'] = cur.fetchone()[0]

    # get number of experiments
    cur.execute("SELECT expid from experimentsTable")
    res = cur.fetchall()
    explist = set()
    for cres in res:
        explist.add(cres[0])
    stats['NumExperiments'] = len(explist)

    cur.execute('SELECT current_database()')
    res = cur.fetchone()
    stats['Database'] = res[0]
    return '', stats
