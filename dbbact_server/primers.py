import psycopg2


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
        print('Error %s' % e)

        # DB exception
        return -2
