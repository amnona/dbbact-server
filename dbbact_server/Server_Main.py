import os

from flask import Flask, g, request
from flask_login import LoginManager, UserMixin

from .autodoc import auto
from .Seq_Flask import Seq_Flask_Obj
from .Exp_Flask import Exp_Flask_Obj
from .Users_Flask import Users_Flask_Obj
from .Docs_Flask import Docs_Flask_Obj
from .DBStats_Flask import DBStats_Flask_Obj
from .Annotation_Flask import Annotation_Flask_Obj
from .Ontology_Flask import Ontology_Flask_Obj
from .utils import debug, SetDebugLevel
from . import db_access
from . import dbuser


# global variables
dbDefaultUser = "na"  # anonymos user in case the field is empty
dbDefaultPwd = ""

recentLoginUsers = []

app = Flask(__name__)
app.register_blueprint(Seq_Flask_Obj)
app.register_blueprint(Exp_Flask_Obj)
app.register_blueprint(Annotation_Flask_Obj)
app.register_blueprint(Ontology_Flask_Obj)
app.register_blueprint(DBStats_Flask_Obj)
app.register_blueprint(Users_Flask_Obj)
app.register_blueprint(Docs_Flask_Obj)

auto.init_app(app)

# setup the user authentication (using json parameters 'user', 'pwd')
login_manager = LoginManager()
login_manager.init_app(app)


class User(UserMixin):

    def __init__(self, username, password, userId, isAdmin):
        self.name = username
        self.password = password
        self.user_id = userId
        self.is_admin = isAdmin


# whenever a new request arrives, connect to the database and store in g.db
@app.before_request
def before_request():
    if request.remote_addr != '127.0.0.1':
        debug(6, 'got request for page %s' % request.url, request=request)
    else:
        debug(1, 'got local request for page %s' % request.url, request=request)
    con, cur = db_access.connect_db(server_type=app.config.get('DBBACT_SERVER_TYPE'),
                                    host=app.config.get('DBBACT_POSTGRES_HOST'),
                                    port=app.config.get('DBBACT_POSTGRES_PORT'),
                                    database=app.config.get('DBBACT_POSTGRES_DATABASE'),
                                    user=app.config.get('DBBACT_POSTGRES_USER'),
                                    password=app.config.get('DBBACT_POSTGRES_PASSWORD'))
    g.con = con
    g.cur = cur


# and when the request is over, disconnect
@app.teardown_request
def teardown_request(exception):
    g.con.close()


# handle the cross-site scripting requests (CORS)
# code from https://stackoverflow.com/questions/25594893/how-to-enable-cors-in-flask-and-heroku
# used for the html interactive heatmaps that need reposnse from the dbbact api from within a browser
@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    # this part from: https://stackoverflow.com/questions/25727306/request-header-field-access-control-allow-headers-is-not-allowed-by-access-contr
    header["Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept"
    return response


# the following function will be called for every request autentication is required
@login_manager.request_loader
def load_user(request):
    try:
        debug(1, '>>>>>>>>>>>load_user login attempt')
        user = None
        alldat = request.get_json()
        if (alldat is not None):
            debug(1, 'login request json: %s' % alldat)
            userName = alldat.get('user')
            password = alldat.get('pwd')
        else:
            userName = None
            password = None
        debug(1, 'username is %s' % userName)

        # use default user name when it was not sent
        if(userName is None and password is None):
            userName = dbDefaultUser  # anonymos user in case the field is empty
            password = dbDefaultPwd

        # check if exist in the recent array first & password didnt change
        # for tempUser in recentLoginUsers:
        #   if( tempUser.name == userName ):
        #       if( tempUser.password == password):
        #           user found, return
        #           debug(1,'user %s already found' % (tempUser.name))
        #           return tempUser
        #       else:
        #           debug(1,'remove user %s since it might that the password was changed' % (tempUser.id))
        #           # user exist but with different password, remove the user and continue login
        #           recentLoginUsers.remove(tempUser)

        # user was not found in the cache memory
        errorMes, userId = dbuser.getUserId(g.con, g.cur, userName, password)
        if userId >= 0:
            debug(1, 'load_user login succeeded userid=%d' % userId)
            errorMes, isadmin = dbuser.isAdmin(g.con, g.cur, userName)
            if isadmin != 1:
                isadmin = 0
            user = User(userName, password, userId, isadmin)
            # add the user to the recent users list
            # for tempUser int recentLoginUsers:
            #   if( tempUser.name == user.name ):
            #       debug(1,'user %s already found' % (user.id))
            # add the user to the list
            # recentLoginUsers.append(user)
        else:
            debug(2, 'user login for user %s failed %s' % (userName, errorMes))
            # login failed, so fallback to default user
            errorMes, userId = dbuser.getUserId(g.con, g.cur, dbDefaultUser, dbDefaultPwd)
            isadmin = 0
            if userId >= 0:
                debug(1, 'logged in as default user userid=%d' % userId)
                user = User(dbDefaultUser, dbDefaultPwd, userId, isadmin)
        return user
    # we need this except for flask-autodoc (it does not have flask.g ?!?!)
    except:
        debug(3, 'exception occured when logging in user. login failed')
        return None


def gunicorn(server_type=None, pg_host=None, pg_port=None, pg_db=None, pg_user=None, pg_pwd=None, debug_level=6):
    '''The entry point for running the api server through gunicorn (http://gunicorn.org/)
    to run dbbact rest server using gunicorn, use:

    gunicorn 'dbbact.Server_Main:gunicorn(server_type='main', debug_level=6)' -b 0.0.0.0:5001 --workers 4 --name=dbbact-rest-api


    Parameters
    ----------
    server_type: str or None, optional
        the server instance running. used for db_access(). can be: 'main','develop','test','local'
        None to use the DBBACT_SERVER_TYPE environment variable instead
    pg_host, pg_port, pg_db, pg_user, pg_pwd: str or None, optional
        str to override the env. variable and server_type selected postgres connection parameters
    debug_level: int, optional
        The minimal level of debug messages to log (10 is max, ~5 is equivalent to warning)

    Returns
    -------
    Flask app
    '''
    SetDebugLevel(debug_level)
    # to enable the stack traces on error
    # (from https://stackoverflow.com/questions/18059937/flask-app-raises-a-500-error-with-no-exception)
    app.debug = True
    debug(6, 'starting dbbact rest-api server using gunicorn, debug_level=%d' % debug_level)
    set_env_params()
    if server_type is not None:
        app.config['DBBACT_SERVER_TYPE'] = server_type
    if pg_host is not None:
        app.config['DBBACT_POSTGRES_HOST'] = pg_host
    if pg_port is not None:
        app.config['DBBACT_POSTGRES_PORT'] = pg_port
    if pg_user is not None:
        app.config['DBBACT_POSTGRES_USER'] = pg_user
    if pg_pwd is not None:
        app.config['DBBACT_POSTGRES_PASSWORD'] = pg_user
    if pg_db is not None:
        app.config['DBBACT_POSTGRES_DATABASE'] = pg_db

    return app


def set_env_params():
    # set the database access parameters
    env_params = ['DBBACT_SERVER_TYPE', 'DBBACT_POSTGRES_HOST', 'DBBACT_POSTGRES_PORT', 'DBBACT_POSTGRES_DATABASE', 'DBBACT_POSTGRES_USER', 'DBBACT_POSTGRES_PASSWORD']
    for cparam in env_params:
            cval = os.environ.get(cparam)
            if cval is not None:
                debug(5, 'using value %s for env. parameter %s' % (cval, cparam))
            app.config[cparam] = cval


if __name__ == '__main__':
    SetDebugLevel(6)
    debug(2, 'starting server')
    set_env_params()
    app.run(port=5001, threaded=True)
