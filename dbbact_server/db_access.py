import os

import psycopg2
import psycopg2.extras

from .utils import debug


def connect_db(database='dbbact', user='dbbact', password='magNiv', port=5432, host='localhost'):
    """
    connect to the postgres database and return the connection and cursor
    input:

    output:
    con : the database connection
    cur : the database cursor
    """
    debug(1, 'connecting to database')
    servertype = 'main'
    try:
        if 'DBBACT_SERVER_TYPE' in os.environ:
            servertype = os.environ['SCDB_SERVER_TYPE'].lower()
            if servertype == 'develop':
                user = 'dbbact_develop'
            elif servertype == 'local':
                pass
            else:
                debug(6, 'DBBACT_SERVER_TYPE env. variable has unrecognized value %s. please use one of ["main", "develop"]. using "main" as default')
        else:
            debug(6, 'DBBACT_SERVER_TYPE env. variable not set. using "main" as default')

        debug(1, 'connecting host=%s, database=%s, user=%s, port=%d' % (host, database, user, port))
        if 'OPENU_FLAG' in os.environ:
            con = psycopg2.connect(database=database, user=user, password=password, port=port)
        else:
            con = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        debug(1, 'connected to database')
        return (con, cur)
    except psycopg2.DatabaseError as e:
        msg = 'Cannot connect to database %s. Error %s' % (servertype, e)
        print(msg)
        raise SystemError(msg)
        return None
