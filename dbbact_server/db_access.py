import psycopg2
import psycopg2.extras

from .utils import debug


def connect_db(server_type='main', database='dbbact', user='dbbact', password='magNiv', port=5432, host=None):
    """
    connect to the postgres database and return the connection and cursor

    Parameters
    ----------
    server_type: str
        type of server to connect to. overrides the other default parameters
        options are:
            'main', 'develop', 'test', 'local'
    database: str, optional
        name of the database to connect to (usually 'dbbact'/'dbbact_develop'/'dbbact_test')

    Returns
    -------
    con : the psycopg database connection
    cur : the psycopg database cursor (DictCursor)
    """
    debug(1, 'connecting to database using server type %s' % server_type)
    if server_type == 'develop':
        user = 'dbbact_develop'
        database = 'dbbact_develop'
    elif server_type == 'test':
        user = 'dbbact_test'
        database = 'dbbact_test'
    elif server_type == 'local':
        host = 'localhost'
    elif server_type == 'main':
        pass
    else:
        debug(6, 'DBBACT_SERVER_TYPE env. variable has unrecognized value %s. please use one of ["main", "develop", "test", "local"]. using "main" as default')

    try:
        debug(1, 'connecting host=%s, database=%s, user=%s, port=%d' % (host, database, user, port))
        if host is None:
            con = psycopg2.connect(database=database, user=user, password=password, port=port)
        else:
            con = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        debug(1, 'connected to database')
        return (con, cur)
    except psycopg2.DatabaseError as e:
        msg = 'Cannot connect to database %s. Error %s' % (server_type, e)
        print(msg)
        raise SystemError(msg)
        return None
