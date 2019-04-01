from collections import defaultdict
import datetime
import psycopg2

from .utils import debug


def GetExperimentIdByVals(con, cur, arrName, arrValue, userid=None, logic='any'):
    """
    Searching for id of an experiment matching the details

    input:
    details : list of 'Name' and list of 'Value'
        the details specific to the experiment (i.e. ("MD5Exp","0x445A2"))
    userid : int (optional)
        the userid asking the query (for private studies)
    logic : str (optional)
        'any' (default) to find matching any of details (union)
        'all' to find matching all details (intersect)
    output:
    errmsg : str
        "" if ok, error msg if error encountered
    expid : int
        experiment id of matching experiments
        -1 - couldn't find
        -2 - more than one exp was find
        None if error encountered
    """
    try:
        expids = None
        
        if len(arrName) == 0:
            debug(7, "GetExperimentID failed 1")
            return 'Number of search fields can\'t be 0', None
        
        if len(arrName) != len(arrValue):
            debug(7, "GetExperimentID failed")
            return 'Number of fields in Name and Value arrays can\'t be different', None
        
        sqlQuery = "SELECT distinct expId from ExperimentsTable where "
        for index in range(len(arrName)):
            dtype = arrName[index].lower()
            value = arrValue[index].lower()
            if index > 0 :
                if logic=='all':
                    sqlQuery = sqlQuery + " AND "
                else:
                    sqlQuery = sqlQuery + " OR "
            sqlQuery = sqlQuery + ('(type = \'%s\' AND value = \'%s\')' % (dtype, value))
        print(sqlQuery)
        
        #Run the query
        cur.execute(sqlQuery)
        
        #If number of rows is bigger than 1, return 0
        data = cur.fetchall()
        if len(data) == 0:
            debug(7, "Can't find experiment")
            return 'Can\'t find experiment', -1
        elif len(data) > 1:
            debug(7, "more than one experiment was found")
            return 'More than one experiment was found', -2

        # Only one was found
        return '', data[0][0]

    except psycopg2.DatabaseError as e:
        debug(7, "GetExperimentID failed 2")
        return '%s' % e, None


def GetExperimentId(con, cur, details, userid=None, logic='any'):
    """
    Get the id of an experiment matching the details

    input:
    details : list of (dtype,value) of str
        the details specific to the experiment (i.e. ("MD5Exp","0x445A2"))
    userid : int (optional)
        the userid asking the query (for private studies)
    logic : str (optional)
        'any' (default) to find matching any of details (union)
        'all' to find matching all details (intersect)
    output:
    errmsg : str
        "" if ok, error msg if error encountered
    expids : list of int
        experimentids of matching experiments
        [] if not found
        None if error encountered
    """
    try:
        expids = None
        for (dtype, value) in details:
            dtype = dtype.lower()
            value = value.lower()
            cur.execute('SELECT expId,private,userId from ExperimentsTable where (type,value)=(%s,%s)', [dtype, value])
            cids = set()
            for cres in cur:
                # if private and different user - ignore
                if cres[1] == 'y':
                    if cres[2] != userid:
                        continue
                cids.add(cres[0])
            if expids is None:
                expids = cids
            else:
                if logic=='all':
                    expids = expids.intersection(cids)
                else:
                    expids = expids.union(cids)
        if len(expids) == 0:
            debug(2, "No experiments found matching all details")
            return '', []
            # return 'No experiment match found for details', []
        if len(expids) > 1:
            debug(2, "Found %d experiments matching details" % len(expids))
        return '', list(expids)
    except psycopg2.DatabaseError as e:
        debug(7, "GetExperimentID failed")
        return '%s' % e, None


def AddExperimentDetails(con, cur, expid, details, userid, private='n', commit=True):
    """
    Add details to an existing or new experiment

    input:
    expid : int or None
        if int, add a detail for the expid
        if None, create a new experiment
    details : list of (type,value) of str
        the details specific to the experiment (i.e. ("MD5Exp","0x445A2"))
    userid : int
        the userid which is adding this detail
    commit : bool (optional)
        True (default) to commit, False to skip the commit

    output:
    expid : int
        the expid for which the details were added
        -1 if expid not found
        -2 if error encountered
    """
# try:
    cdate = datetime.date.today().isoformat()
    if expid is None:
        cur.execute("SELECT nextval('ExperimentsTable_expId_seq')")
        expid = cur.fetchone()[0]
    else:
        # test if expid exists
        if not TestExpIdExists(con, cur, expid):
            debug(5, "expid %d does not exist" % expid)
            return -1
    print(details)
    for ctype, cval in details:
        ctype = ctype.lower()
        cval = cval.lower()
        cur.execute('INSERT INTO ExperimentsTable (expId,type,value,date,userid,private) VALUES(%s,%s,%s,%s,%s,%s)', [expid, ctype, cval, cdate, userid, private])
    if commit:
        con.commit()
    return expid
# except:
    debug(7, "AddExperimentDetails failed")
    return -2


def TestExpIdExists(con, cur, expid, userid=None):
    """
    test if expid exists in table

    input:
    expid : int
    userid : int (optional)

    output:
    True if exists and not private or (private and userid match), False if does not exist, or error encountered
    """
# try:
    debug(1, "TestExpIdExists %d userid %s" % (expid, userid))
    cur.execute('SELECT private,userId from ExperimentsTable where expId=%s LIMIT 1', [expid])
    if cur.rowcount == 0:
        debug(2, "expid %d does not exist" % expid)
        return False
    res = cur.fetchone()
    # if not private - say it exists
    if res[0] == 'n':
        debug(1, "expid %d public - exists" % expid)
        return True
    # if private and same user - say it exists
    if res[1] == userid:
        debug(1, "expid %d private but same user - exists" % expid)
        return True
    # so private and different user - say it does not exist
    debug(1, "expid %d private and different users (created by %d, queried by %s). Say it doesn't exist" % (expid, res[1], userid))
    return False
# except:
    debug(7, "error in TestExpIdExists")
    return False


def GetDetailsFromExpId(con, cur, expid, userid=None):
    """
    get the details of an experiment with id expid

    input:
    con,cur
    expid : int
        the experiment id
    userid : int (optional)
        the userid of the query (or None for anonymous user)

    output:
    err : str
        the error msg or '' if no error encountered
    details : list of (str,str)
    list of (type,value) of the experiment details
    """
    debug(1, 'GetDetailsFromExpId %d' % expid)
    cur.execute('SELECT type,value from ExperimentsTable WHERE expId=%s', [expid])
    if cur.rowcount == 0:
        debug(3, 'Experiment %d not found for GetDetailsFromExpId' % expid)
        return "Experiment %d not found" % expid, -1
    details = []
    for cdetail in cur:
        details.append([cdetail[0], cdetail[1]])
    debug(2, 'Found %d details for expid %d' % (len(details), expid))
    return '', details


def GetExperimentsList(con, cur, userid=None):
    '''Get the list of experiments in the database and the details about each one

    Parameters
    ----------
    con, cur
    userid : int (optional)
        the userid of the query (or None for anonymous user)

    Returns
    -------
    err : str
        the error msg or '' if no error encountered
    explist : list of (expid, expdetails) where expdetails is a list of (name, value)
        the details about each experiment
    '''
    debug(1, 'GetExperimentsList')
    explist = defaultdict(list)
    cur.execute('SELECT expid,userid,private,type,value from ExperimentsTable')
    res = cur.fetchall()
    for cres in res:
        cid = cres[0]
        cuserid = cres[1]
        cprivate = cres[2]
        ctype = cres[3]
        cvalue = cres[4]
        if cprivate == 'y':
            if userid != cuserid:
                debug(1, 'experiment %d is private and not the creating user. skipping' % cid)
                continue
        explist[cid].append([ctype, cvalue])
    explist = list(explist.items())
    return '', explist
