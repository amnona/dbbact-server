from .utils import debug


def GetStats(con, cur):
    """
    Get statistics about the database
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

    return '', stats
