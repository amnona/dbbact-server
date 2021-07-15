import json
from flask import Blueprint, request, g
from flask_login import login_required, current_user
from . import dbannotations
from . import dbuser
from .utils import debug, getdoc, send_email, random_str
from .autodoc import auto


Users_Flask_Obj = Blueprint('Users_Flask_Obj', __name__, template_folder='templates')

MAX_RECOVERY_ATTEMPTS = 10


@Users_Flask_Obj.route('/users/get_user_id', methods=['POST', 'GET'])
@auto.doc()
def get_user_id():
    """
    Title: Get user id from user/password
    URL: users/get_user_id
    Method: POST
    URL Params:
    Data Params: JSON
        {
            user : str
                user name
            pwd : str
                password
        }
    Success Response:
        Code : 200
        Content :
        {
            user : int
                the userid (0 for anonymous if user and pwd are empty)
        }
    """

    cfunc = get_user_id
    if request.method == 'GET':
        return(getdoc(cfunc))
    alldat = request.get_json()
    user = alldat.get('user')
    pwd = alldat.get('pwd')

    err, userid = dbuser.getUserId(g.con, g.cur, user, pwd)
    if err:
        return(err, 400)
    debug(2, err)
    return json.dumps({"user": userid})


@Users_Flask_Obj.route('/users/get_user_public_information', methods=['POST', 'GET'])
@auto.doc()
def get_user_public_information():
    """
    Title: Return the user information
    URL: users/get_user_public_information
    Method: POST,GET
    Data Params: JSON
        {
            username : str
                the dbbact username
        }
    Success Response:
        Code : 200
        Content :
        {
            data: dict
                the user data. includes:
                'id' : int
                'username' : str
                'name' : str
                'description' : str
                'email' : str
                    if shared, returns email, otherwise returns "-"
        }
    """

    cfunc = get_user_public_information
    if request.method == 'GET':
        return(getdoc(cfunc))
    alldat = request.get_json()
    username = alldat.get('username')

    err, userinfo = dbuser.getUserInformation(g.con, g.cur, username)
    if err:
        return(err, 400)
    debug(2, err)
    return json.dumps(userinfo)


@Users_Flask_Obj.route('/users/test_user_login', methods=['POST', 'GET'])
@auto.doc()
@login_required
def test_user_login():
    """
    Title: test user login
    URL: users/test_user_login
    Method: POST/GET
    """

    debug(6, 'login succeed. id=%s' % current_user.user_id)

    return 'login succeed. id=%s' % current_user.user_id


@Users_Flask_Obj.route('/users/register_user', methods=['POST', 'GET'])
@auto.doc()
def register_user():
    """
    Title: register new user
    URL: /users/register_user
    Method: POST
    URL Params:
    Data Params: JSON
        {
            'user': str
                user name
            'pwd': str,
                password
            'name': str
                name (optional)
            'description': str
                description (optional)
            'email': str
                email address
            'publish': 'y' or 'n'
                publish user email
        }
    Success Response:
        Code : 201
        Content :
        {
            "status" : 1
        }
    Details:
        Validation:
        Action:
    """
    cfunc = register_user
    if request.method == 'GET':
        return(getdoc(cfunc))
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))

    user = alldat.get('user')
    pwd = alldat.get('pwd')
    name = alldat.get('name')
    description = alldat.get('description')
    mail = alldat.get('email')
    publish = alldat.get('publish')

    err, retval = dbuser.addUser(g.con, g.cur, user, pwd, name, description, mail, publish)
    if retval <= 0:
        return(err, 400)
    debug(2, 'Added user completed successfully')
    return json.dumps({"status": 1})


@Users_Flask_Obj.route('/users/forgot_password', methods=['POST', 'GET'])
@auto.doc()
def forgot_password():
    """
    Title: send passowrd via mail
    URL: /users/forgot_password
    Method: POST
    URL Params:
    Data Params: JSON
        {
            'user' : str
                user name
        }
    Success Response:
        Code : 201
        Content :
        {
            "status" : 1
        }
    Details:
        Validation:
        Action:
    """
    debug(3, 'forgot_password')
    cfunc = forgot_password
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))

    user = alldat.get('user')
    err, retval = dbuser.getMail(g.con, g.cur, user)
    if retval <= 0:
        return(err, 400)
    email = err
    # generate and update new password
    newpassword = random_str()
    debug(3, 'calling updateNewTempcode')
    err, retval = dbuser.updateNewTempcode(g.con, g.cur, user, newpassword)
    if retval <= 0:
        return(err, 400)

    guser = "bactdb@gmail.com"
    gpassword = "databaseforbacteria"
    recipient = email
    subject = "Password reset"
    body = "Your password recovery code is: " + newpassword
    debug(3, 'Sending mail to %s' % email)
    send_email(guser, gpassword, recipient, subject, body)
    debug(3, 'New password sent')
    return json.dumps({"status": 1})


@Users_Flask_Obj.route('/users/recover_password', methods=['POST', 'GET'])
@auto.doc()
def recover_password():
    """
    Title: send passowrd via mail
    URL: /users/recover_password
    Method: POST
    URL Params:
    Data Params: JSON
        {
            'user' : str
                user name
        }
    Success Response:
        Code : 201
        Content :
        {
            "status" : 1
        }
    Details:
        Validation:
        Action:
    """
    cfunc = recover_password
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))

    user = alldat.get('user')
    recoverycode = alldat.get('recoverycode')
    newpassword = alldat.get('newpassword')

    # Get the old recovery counter
    count = dbuser.getUserRecoveryAttemptsByName(g.con, g.cur,user)
    if count < 0 :
        return('failed to get recovery counter', 400)

    if count >= MAX_RECOVERY_ATTEMPTS:
        return('User is locked', 400)

    err, userid = dbuser.getUserIdRecover(g.con, g.cur, user, recoverycode)
    if err:
        count = count + 1
        dbuser.setUserRecoveryAttemptsByName(g.con, g.cur, user, count)
        return(err, 400)

    # generate and update new password
    err, retval = dbuser.updateNewPassword(g.con, g.cur, user, newpassword)
    if retval <= 0:
        return(err, 400)

    # reset the login attempts
    dbuser.setUserLoginAttemptsByName(g.con, g.cur, user, 0)
    dbuser.setUserRecoveryAttemptsByName(g.con, g.cur, user, 0)


@Users_Flask_Obj.route('/users/get_user_annotations', methods=['GET'])
@auto.doc()
def get_user_annotations():
    """
    Title: Get user annotations
    Description : Get all the annotations created by a user
    URL: /sequences/get_user_annotations
    Method: GET
    URL Params:
    Data Params: JSON
        {
            foruserid : int
                the userid to get the annotations created by
    Success Response:
        Code : 200
        Content :
        {
            'userannotations': list
            list of:
                {
                "taxonomy" : str
                (taxonomy from SequencesTable)
                "annotations" : list of
                    {
                        "annotationid" : int
                            the id of the annotation
                        "user" : str
                            name of the user who added this annotation
                            (userName from UsersTable)
                        "addedDate" : str (DD-MM-YYYY HH:MM:SS)
                            date when the annotation was added
                            (addedDate from CurationsTable)
                        "expid" : int
                            the ID of the experiment from which this annotation originated
                            (uniqueId from ExperimentsTable)
                            (see Query Experiment)
                        "currType" : str
                            curration type (differential expression/contaminant/etc.)
                            (description from CurationTypesTable)
                        "method" : str
                            The method used to detect this behavior (i.e. observation/ranksum/clustering/etc")
                            (description from MethodTypesTable)
                        "agentType" : str
                            Name of the program which submitted this annotation (i.e. heatsequer)
                            (description from AgentTypesTable)
                        "description" : str
                            Free text describing this annotation (i.e. "lower in green tomatoes comapred to red ones")
                        "private" : bool
                            True if the curation is private, False if not
                        "CurationList" : list of
                            {
                                "detail" : str
                                    the type of detail (i.e. ALL/HIGH/LOW)
                                    (description from CurationDetailsTypeTable)
                                "term" : str
                                    the ontology term for this detail (i.e. feces/ibd/homo sapiens)
                                    (description from OntologyTable)
                            }
                    }
                }
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the annotation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    cfunc = get_user_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    foruserid = alldat.get('foruserid')
    if foruserid is None:
        return('foruserid parameter missing', 400)
    err, userannotations = dbannotations.GetUserAnnotations(g.con, g.cur, foruserid=foruserid)
    if err:
        debug(6, err)
        return ('Problem geting user annotation details. error=%s' % err, 400)

    return json.dumps({'userannotations': userannotations})


"""
@Users_Flask_Obj.route('/users/send_mail_test',methods=['POST','GET'])
def send_mail_test():

    user = "bactdb@gmail.com"
    password = "databaseforbacteria"
    recipient = "eitanozel@gmail.com"
    subject = "test2"
    body = "test3"

    cfunc=send_mail_test
    if request.method=='GET':
        return(getdoc(cfunc))

    send_email(user,password,recipient,subject,body)
    return json.dumps({"status":1})

@Users_Flask_Obj.route('/users/add_temp_users',methods=['POST','GET'])
def add_temp_users():

    cfunc=add_temp_users
    if request.method=='GET':
        return(getdoc(cfunc))
    alldat=request.get_json()

    err=dbuser.addTempUsers(g.con,g.cur)
    if err:
        return(err,400)
    debug(2,'Added temp user completed successfully')
    return ""

@Users_Flask_Obj.route('/users/add_temp_users2',methods=['POST','GET'])
def add_temp_users2():

    cfunc=add_temp_users2
    if request.method=='GET':
        return(getdoc(cfunc))
    alldat=request.get_json()

    err=dbuser.addTempUsers2(g.con,g.cur)
    if err:
        return(err,400)
    debug(2,'Added temp user completed successfully')
    return ""
"""
