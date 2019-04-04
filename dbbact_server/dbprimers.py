import psycopg2

from .utils import debug


def get_primers(con, cur):
    '''Get information about all the sequencing primers used in dbbact

    Returns
    -------
    primers: list of dict of {
        'primerid': int
            dbbact internal id of the primer region (i.e. 1 for v4, etc.)
        'name': str,
            name of the primer region (i.e. 'v4', 'its1', etc.)
        'fprimer': str
        'rprimer: str
            name of the forward and reverse primers for the region (i.e. 515f, etc.)
    '''
    debug(1, 'get_primers')

    primers = []
    cur.execute('SELECT id, regionname, forwardprimer, reverseprimer FROM PrimersTable')
    res = cur.fetchall()
    for cres in res:
        cprimer = {}
        cprimer['primerid'] = cres[0]
        cprimer['name'] = cres[1]
        cprimer['fprimer'] = cres[2]
        cprimer['rprimer'] = cres[3]
        primers.append(cprimer)
    debug(1, 'found %d primers' % len(primers))
    return '', primers


def GetIdFromName(con, cur, name):
    """
    get id of primer based on regionName

    input:
    regionName : str
        name of the primer region (i.e. 'V4')

    output:
    id : int
        the id of the region (>0)
        -1 if region not found
        -2 if database error
    """
    name = name.lower()
    try:
        cur.execute('SELECT id from PrimersTable where regionName=%s', [name])
        rowCount = cur.rowcount
        if rowCount == 0:
            # region not found
            return -1
        else:
            # Return the id
            res = cur.fetchone()
            return res[0]
    except psycopg2.DatabaseError as e:
        debug(4, 'Error %s' % e)
        # DB exception
        return -2


def AddPrimerRegion(con, cur, regionname, forwardprimer='', reverseprimer='', userid=None, commit=True):
    '''Add a new region to the primers table

    Parameters
    ----------
    regionname: str
        The name of the primer region to add (i.e 'V4')
    forward_primer, reverse_primer: str, optional
        name (i.e. 515f) or sequence of the corresponding primer used to amplify the region
    userid: int, optional
        the user adding the primer

    Returns
    -------
    empty string('') if ok, error string if error encountered
    '''
    regionname = regionname.lower()
    forwardprimer = forwardprimer.lower()
    reverseprimer = reverseprimer.lower()
    cid = GetIdFromName(con, cur, regionname)
    if cid != -1:
        debug(2, 'region %s already exists in PrimersTable' % regionname)
        return 'region %s already exists in PrimersTalbe' % regionname
    try:
        cur.execute('INSERT INTO PrimersTable (regionname, forwardprimer, reverseprimer, iduser) VALUES (%s, %s, %s, %s)', [regionname, forwardprimer, reverseprimer, userid])
        if commit:
            con.commit()
        debug(2, 'primer region %s added' % regionname)
        return ''
    except psycopg2.DatabaseError as e:
        debug(4, 'Database error %s encountered when adding primer region %s' % (e, regionname))
        return 'db error when adding primer region'
