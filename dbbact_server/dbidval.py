import psycopg2
from .utils import debug


def GetIdFromDescription(con, cur, table, description, noneok=False, addifnone=False, commit=True):
    """
    Get the id based on a value for a given table (and add if doesn't exists if addifnone=True)

    input:
    con,cur
    table : str
        Name of the table to search
    value : str
        The value to search for
    noneok : bool (optional)
        False (default) fails if value is None, True returns 0 if None encountered
    addifnone : bool (optional)
        False (default) to return without adding if item does not exist. True to add if item does not exist
    commit : bool (optional)
        True (default) to commit if adding new item, False to skip commit

    output:
    cid : int
        the id of the value, -1 if not found, -2 if error
    """
    try:
        if description is None:
            if noneok:
                return 0
            else:
                return -1
        description = description.lower()
        cur.execute('SELECT id from %s WHERE description=%s LIMIT 1' % (table, '%s'), [description])
        if cur.rowcount == 0:
            if not addifnone:
                debug(2, "value %s not found in table %s" % (description, table))
                return -1
            err, cid = AddItem(con, cur, table, description, allowreplicate=True, commit=commit)
            if err:
                debug(7, 'error when adding term: %s' % err)
                return -2
            debug(2, 'new term added to table. reported as found')
            return cid
        cid = cur.fetchone()[0]
        debug(2, "value %s found in table %s id %d" % (description, table, cid))
        return cid
    except psycopg2.DatabaseError as e:
        debug(8, "error %s in GetIdFromValue" % e)
        return -2


def AddItem(con, cur, table, description, allowreplicate=False, commit=True):
    """
    Add a id,description to table and return the id.
    If item already exists, behavior depends on allowreplicate:
        False (default) - just return the id of the existing item
        True - add a new item

    input:
    con,cur
    table : str
        Name of the table to search
    description : str
        the description to add
    allowreplicate : bool (optional)
        False (default) - just return the id of the existing item
        True - add a new item
    commit : bool (optional)
        True (default) to commit, False to skip the commit

    output:
    err : str
        Error message or empty string if ok
    sid : int
        the id of the added item
    """
    print('nana')
    print(table)
    try:
        description = description.lower()
        if not allowreplicate:
            # search if exists
            sid = GetIdFromDescription(con, cur, table, description)
            if sid >= 0:
                debug(2, 'AddItem - item %s already exists. id is %d' % (description, sid))
                return '', sid
        # should create new item
        print('pookapooka')
        print('adding to table %s term %s' % (table, description))
        cur.execute('INSERT INTO %s (description) VALUES (%s) RETURNING id' % (table, '%s'), [description])
        sid = cur.fetchone()[0]
        debug(2, 'AddItem - added new item %s. id is %d' % (description, sid))
        if commit:
            con.commit()
        return '', sid
    except psycopg2.DatabaseError as e:
        debug(8, "error %s in AddItem" % e)
        return 'Error %s in AddItem' % e, -2


def GetDescriptionFromId(con, cur, table, cid):
    """
    Get the description for id cid in table

    input:
    con,cur
    table : str
    table : str
        Name of the table to search
    cid : int
        the id to get the description for

    output:
    err : str
        Error message or empty string if ok
    description : str
        the description of the id
    """
    try:
        cur.execute('SELECT description FROM %s WHERE id=%s LIMIT 1' % (table, '%s'), [cid])
        if cur.rowcount == 0:
            debug(2, 'id not found in table %s' % table)
            return 'id not found in table %s' % table, ''
        description = cur.fetchone()[0]
        debug(1, 'found description %s for id %d in table %s' % (description, cid, table))
        return '', description
    except psycopg2.DatabaseError as e:
        debug(8, "error %s in GetDescriptionFromId" % e)
        return 'Error %s in GetDescriptionFromId' % e, ''
