from flask import Flask, g, request
from flask_login import LoginManager, UserMixin, login_required

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
import os


# global variables
dbDefaultUser = "na"  # anonymos user in case the field is empty
dbDefaultPwd = ""

# this variable stores the server type to use. since we connect to the database every request, need to store between them the inital server type.
server_type_g = None

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
    global server_type_g
    if request.remote_addr != '127.0.0.1':
        debug(6, 'got request for page %s' % request.url, request=request)
    else:
        debug(1, 'got local request for page %s' % request.url, request=request)
    con, cur = db_access.connect_db(server_type=server_type_g)
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
            debug(1, 'user login failed %s' % (errorMes))
            # login failed, so fallback to default user
            errorMes, userId = dbuser.getUserId(g.con, g.cur, dbDefaultUser, dbDefaultPwd)
            isadmin = 0
            if userId >= 0:
                debug(1, 'logged in as default user userid=%d' % userId)
                user = User(dbDefaultUser, dbDefaultPwd, userId, isadmin)
        return user
    # we need this except for flask-autodoc (it does not have flask.g ?!?!)
    except:
        return None


def gunicorn(server_type=None, debug_level=6):
    '''The entry point for running the api server through gunicorn (http://gunicorn.org/)
    to run dbbact rest server using gunicorn, use:

    gunicorn 'dbbact.Server_Main:gunicorn(server_type='main', debug_level=6)' -b 0.0.0.0:5001 --workers 4 --name=dbbact-rest-api


    Parameters
    ----------
    server_type: str or None, optional
        the server instance running. used for db_access(). can be: 'main','develop','test','local'
        None to use the DBBACT_SERVER_TYPE environment variable instead
    debug_level: int, optional
        The minimal level of debug messages to log (10 is max, ~5 is equivalent to warning)

    Returns
    -------
    Flask app
    '''
    global server_type_g

    SetDebugLevel(debug_level)
    # to enable the stack traces on error
    # (from https://stackoverflow.com/questions/18059937/flask-app-raises-a-500-error-with-no-exception)
    app.debug = True
    debug(6, 'starting dbbact rest-api server using gunicorn, debug_level=%d' % debug_level)
    if server_type is None:
        if 'DBBACT_SERVER_TYPE' in os.environ:
            server_type = os.environ['DBBACT_SERVER_TYPE']
            debug(6, 'Using server_type %s from DBBACT_SERVER_TYPE env. variable' % server_type)
        else:
            debug(7, 'server_type not supplied and DBBACT_SERVER_TYPE env. variable not set.')
    else:
        if 'DBBACT_SERVER_TYPE' in os.environ:
            debug(7, 'server_type supplied but DBBACT_SERVER_TYPE env. variable also set. Using server_type %s' % server_type)
    server_type_g = server_type
    return app


if __name__ == '__main__':
    SetDebugLevel(6)
    debug(2, 'starting server')
    if 'DBBACT_SERVER_TYPE' in os.environ:
        server_type_g = os.environ['DBBACT_SERVER_TYPE']
    else:
        debug(7, 'server_type not supplied and DBBACT_SERVER_TYPE env. variable not set.')
    app.run(port=5001, threaded=True)
    # if 'OPENU_FLAG' in os.environ:
    #     app.run(host='0.0.0.0', port=5001, threaded=True)
    # else:
    #     app.run(threaded=True)
