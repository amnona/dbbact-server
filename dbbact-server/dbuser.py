import psycopg2
from .utils import debug

maxfailedattempt = 3


def getUserId(con, cur, user, password):
    """
    Get the user id after autontication

    input:
    con,cur : database connection and cursor
    user : user name
    pasword: user password

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        -1 if the user doesnt exist
        -2 if the user exist but using wrong password
        -3 user is locked
        -4 exception
        user id  >= 0 if the user exist
    """
    try:
        debug(1, 'SELECT id,attemptscounter FROM userstable WHERE username=%s' % user)
        cur.execute('SELECT id,attemptscounter FROM userstable WHERE username=%s', [user])
        if cur.rowcount == 0:
            debug(3, 'user %s was not found in userstable' % [user])
            return 'user %s was not found in userstable' % [user], -1
        else:
            # save the user id
            row = cur.fetchone()
            tempUserId = row[0]
            failAttemptCounter = row[1]
            if failAttemptCounter >= maxfailedattempt:
                # user exist but is currently locked
                debug(5, 'user %s is locked after several login attempts' % [user])
                return 'user %s is locked after several login attempts' % [user], -3
            # user exist and not locked , try to log in
            cur.execute('SELECT id FROM userstable WHERE (username=%s and passwordhash = crypt(%s, passwordhash))', [user, password])
            if cur.rowcount == 0:
                # increase the failure attempt counter
                failAttemptCounter = failAttemptCounter + 1
                debug(3, 'increment failed attempt for user %s , fail attempt = %s' % (user, failAttemptCounter))
                setUserLoginAttempts(con, cur, tempUserId, failAttemptCounter)
                debug(3, 'invalid password for user %s' % [user])
                return 'invalid password for user %s' % [user], -2
            else:
                userId = cur.fetchone()[0]
                debug(1, 'login succeed for user %s' % [user, userId])
                if failAttemptCounter != 0:
                    debug(3, 'reset failed attempt for user %s' % [userId])
                    setUserLoginAttempts(con, cur, userId, 0)

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in GetUserId" % e)
        return "error %s enountered in GetUserId" % e, -4

    return "", userId

def getUserIdRecover(con, cur, user, recoverycode):
    """
    Get the user id using recover code

    input:
    con,cur : database connection and cursor
    user : user name
    recoverycode: user recovery code

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        -1 if the user doesnt exist
        -2 if the user exist but using wrong password
        -3 user is locked
        -4 exception
        user id  >= 0 if the user exist
    """
    try:
        debug(1, 'SELECT id FROM userstable WHERE username=%s' % user)
        cur.execute('SELECT id FROM userstable WHERE username=%s', [user])
        if cur.rowcount == 0:
            debug(3, 'user %s was not found in userstable' % [user])
            return 'user %s was not found in userstable' % [user], -1
        else:
            # save the user id
            row = cur.fetchone()
            tempUserId = row[0]
            
            # user exist and not locked , try to log in
            debug(3,'SELECT id FROM userstable WHERE (username=%s and tempcodehash = crypt(%s, tempcodehash))' % (user, recoverycode))
            cur.execute('SELECT id FROM userstable WHERE (username=%s and tempcodehash = crypt(%s, tempcodehash))', [user, recoverycode])
            if cur.rowcount == 0:
                # increase the failure attempt counter
                debug(3, 'invalid recovery code for user %s' % [user])
                return 'invalid recovery code for user %s' % [user], -2
            else:
                userId = cur.fetchone()[0]
                debug(1, 'login succeed for user %s' % [user, userId])

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in getUserIdRecover" % e)
        return "error %s enountered in getUserIdRecover" % e, -4

    return "", userId

def isUserExist(con, cur, user):
    """
    Check if user name is already in use

    input:
    con,cur : database connection and cursor
    user : user name

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        1 user exist
        0 user doesnt exist
        -4 exception
    """
    try:
        debug(1, 'SELECT id,attemptscounter FROM userstable WHERE username=%s' % user)
        cur.execute('SELECT id,attemptscounter FROM userstable WHERE username=%s', [user])
        if cur.rowcount == 0:
            debug(3, 'user %s was not found in userstable' % [user])
            return "", 0
        else:
            return "", 1

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in GetUserId" % e)
        return "error %s enountered in GetUserId" % e, -4


def isAdmin(con, cur, user):
    """
    Check if user is admin

    input:
    con,cur : database connection and cursor
    user : user name

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        1 user is admin
        0 user is not admin
        -1 user was not found
        -4 exception
    """
    try:
        debug(1, 'SELECT isadmin FROM userstable WHERE username=%s' % user)
        cur.execute('SELECT isadmin FROM userstable WHERE username=%s', [user])
        if cur.rowcount == 0:
            debug(3, 'user %s was not found in userstable' % [user])
            return 'user %s was not found in userstable' % [user], -1
        else:
            admin = cur.fetchone()[0]
            if admin and not admin.isspace():
                if admin == "y":
                    return "", 1
            return "", 0

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in GetUserId" % e)
        return "error %s enountered in GetUserId" % e, -4


def setUserLoginAttempts(con, cur, usrid, val):
    """
    Set user login attempt

    input:
    con,cur : database connection and cursor
    user : user id
    val: number of login attempt

    output:
    """
    returnVal = 0
    debug(3, 'update userstable set attemptscounter=%s WHERE id=%s' % (val, usrid))
    cur.execute('update userstable set attemptscounter=%s WHERE id=%s', [val, usrid])
    con.commit()


def setUserLoginAttemptsByName(con, cur, username, val):
    """
    Set user login attempt

    input:
    con,cur : database connection and cursor
    username : user name
    val: number of login attempt

    output:
    """
    returnVal = 0
    debug(3, 'update userstable set attemptscounter=%s WHERE username=%s' % (val, username))
    cur.execute('update userstable set attemptscounter=%s WHERE username=%s', [val, username])
    con.commit()

def setUserRecoveryAttemptsByName(con, cur, username, val):
    """
    Set user login recovery attempts

    input:
    con,cur : database connection and cursor
    username : user name
    val: number of login attempt

    output:
    """
    returnVal = 0
    debug(3, 'update userstable set recoveryattemptscounter=%s WHERE username=%s' % (val, username))
    cur.execute('update userstable set recoveryattemptscounter=%s WHERE username=%s', [val, username])
    con.commit()

def getUserLoginAttempts(con, cur, usrid):
    """
    Set user login attempt

    input:
    con,cur : database connection and cursor
    user : user id

    output:
    number of attempts : int
        return the number of failed attempts for a given user
    """
    returnVal = 0
    cur.execute('SELECT attemptscounter FROM userstable WHERE id=%s', [usrid])
    if cur.rowcount == 0:
        debug(6, 'cant find user %s' % [user])
        returnVal = 0
    else:
        returnVal = cur.fetchone()[0]
    return returnVal

def getUserRecoveryAttempts(con, cur, usrid):
    """
    Get user login attempt

    input:
    con,cur : database connection and cursor
    user : user id

    output:
    number of attempts : int
        return the number of failed attempts for a given user
    """
    returnVal = 0
    cur.execute('SELECT recoveryattemptscounter FROM userstable WHERE id=%s', [usrid])
    if cur.rowcount == 0:
        debug(6, 'cant find user %s' % [user])
        returnVal = 0
    else:
        returnVal = cur.fetchone()[0]
        if not returnVal:
            returnVal = 0
    return returnVal

def getUserRecoveryAttemptsByName(con, cur, name):
    """
    Get user login attempt

    input:
    con,cur : database connection and cursor
    user : user id

    output:
    number of attempts : int
        return the number of failed attempts for a given user
    """
    returnVal = 0
    cur.execute('SELECT recoveryattemptscounter FROM userstable WHERE username=%s', [name])
    if cur.rowcount == 0:
        debug(6, 'cant find user %s' % [name])
        returnVal = -1
    else:
        returnVal = cur.fetchone()[0]
        if not returnVal:
            returnVal = 0
    return returnVal

def addUser(con, cur, user, pwd, name, description, mail, publish):
    """
    Add new user

    input:
    con,cur : database connection and cursor
    user : user name
    pwd: user password
    name: name
    description: description (optional)
    mail: user email
    publish: publish user mail ('y' or 'n')

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        1 operaion ended succesfully
        -1 empty user
        -2 empty password
        -3 user already exist
        -4 exception
    """
    if user == "":
        return ("user can't be empty", -1)
    if pwd == "":
        return ("pwd can't be empty", -2)

    # If the user already exist, return error
    err, val = isUserExist(con, cur, user)
    if val > 0:
        return ("user %s already exist" % user, -3)

    # default values
    isactive = "y"
    attemptscounter = 0
    try:
        debug(3, "insert into userstable (username, passwordhash,name,description,isactive,shareemail,email,attemptscounter) values (%s, crypt(%s, gen_salt('bf')), %s, %s , %s, %s, %s, %s)" % (user, pwd, name, description, isactive, publish, mail, attemptscounter))
        cur.execute("insert into userstable (username, passwordhash,name,description,isactive,shareemail,email,attemptscounter) values (%s, crypt(%s, gen_salt('bf')), %s, %s , %s, %s, %s, %s)", [user, pwd, name, description, isactive, publish, mail, attemptscounter])
        con.commit()
        return "", 1
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in addUser" % e)
        return ("error %s enountered in addUser" % e, -4)


def updateNewPassword(con, cur, user, newpwd):
    """
    Update password for user

    input:
    con,cur : database connection and cursor
    user : user name
    newpwd: update user password

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        1 operaion ended succesfully
        -1 empty user
        -2 empty password
        -3 user doesnt exist
        -4 exception
    """
    if user == "":
        return ("user can't be empty", -1)
    if newpwd == "":
        return ("pwd can't be empty", -2)

    # If the user already exist, return error
    err, val = isUserExist(con, cur, user)
    if val < 0:
        return ("user %s doesnt exist" % user, -3)

    # default values
    try:
        debug(3, "update userstable set passwordhash = crypt(%s, gen_salt('bf')) where username=%s" % (newpwd, user))
        cur.execute("update userstable set passwordhash = crypt(%s, gen_salt('bf')) where username=%s", [newpwd, user])
        con.commit()
        return "", 1
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in addUser" % e)
        return ("error %s enountered in addUser" % e, -4)

def updateNewTempcode(con, cur, user, tempcode):
    """
    Update tempCode for user

    input:
    con,cur : database connection and cursor
    user : user name
    newpwd: temporary code used for reset password

    output:
    errmsg : str
        "" if ok, error msg if error encountered
    id : int
        1 operaion ended succesfully
        -1 empty user
        -2 empty password
        -3 user doesnt exist
        -4 exception
    """
    if user == "":
        return ("user can't be empty", -1)
    if tempcode == "":
        return ("tempcode can't be empty", -2)

    # If the user already exist, return error
    err, val = isUserExist(con, cur, user)
    if val < 0:
        return ("user %s doesnt exist" % user, -3)

    # default values
    try:
        debug(3, "update userstable set tempcodehash = crypt(%s, gen_salt('bf')) where username=%s" % (tempcode, user))
        cur.execute("update userstable set tempcodehash = crypt(%s, gen_salt('bf')) where username=%s", [tempcode, user])
        con.commit()
        return "", 1
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in addUser" % e)
        return ("error %s enountered in addUser" % e, -4)


def getMail(con, cur, user):
    """
    Get mail

    input:
    con,cur : database connection and cursor
    user : user name

    output:
    errmsg : str
        empty ok, error msg if error encountered
    email : str
        email address if ok, error msg if error encountered
    password : str
        email address if ok, error msg if error encountered
    id : int
        1 operaion ended succesfully
        -1 empty user
        -2 user doesnt exist
        -3 mail doesnt exist
        -4 exception

    """
    if user == "":
        return ("user can't be empty", -1)
    # If the user already exist, return error
    err, val = isUserExist(con, cur, user)
    if val == 0:
        return ("user %s doesnt exist" % user, -2)
    # default values
    try:
        debug(1, 'SELECT email FROM userstable WHERE username=%s' % user)
        cur.execute('SELECT email FROM userstable WHERE username=%s', [user])
        if cur.rowcount > 0:
            email = cur.fetchone()[0]
            if email and not email.isspace():
                return (email, 1)
            else:
                return ("user %s - email is empty" % user, -3)
        else:
            return ("user %s doesnt exist" % user, -2)

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in addUser" % e)
        return ("error %s enountered in addUser" % e, -4)


def getUserInformation(con, cur, userid):
    """
    Get the public information of user

    input:
    con,cur : database connection and cursor
    user: id

    output:
    errmsg : str
        empty ok, error msg if error encountered
    data: dict
        the user data. includes:
        'id' : int
        'username' : str
        'name' : str
        'description' : str
        'email' : str
    """
    if userid is not None and userid < 0:
        return ("user id can't be negative", None)

    try:
        debug(1, 'SELECT * userstable WHERE id=%s' % userid)
        cur.execute('SELECT * FROM userstable WHERE id=%s', [userid])
        if cur.rowcount > 0:
            data = {}
            res = cur.fetchone()
            data['id'] = userid
            data['username'] = res['username']
            data['name'] = res['name']
            data['description'] = res['description']

            sharemail = res['shareemail']
            if sharemail is not None and sharemail == 'y':
                data['email'] = res['email']
            else:
                data['email'] = '-'
            return ('', data)
        else:
            return ("user %s doesnt exist" % userid, None)

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in getUserInformation" % e)
        return ("error %s enountered in getUserInformation" % e, None)


"""
def addTempUsers(con,cur):

    pwd1 = "ptest1"
    pwd2 = "ptest2"
    pwd3 = "ptest3"

    sql1 = "insert into  annotationschematest.userstable (username, passwordhash,name,description,isactive) values ('user1', crypt('puser1', gen_salt('bf')), 'user1 name', 'user1 description' , true)"
    sql2 = "insert into  annotationschematest.userstable (username, passwordhash,name,description,isactive) values ('user2', crypt('puser2', gen_salt('bf')), 'user2 name', 'user2 description' , true)"
    sql3 = "insert into  annotationschematest.userstable (username, passwordhash,name,description,isactive) values ('user3', crypt('puser3', gen_salt('bf')), 'user3 name', 'user3 description' , true)"
    try:
        cur.execute(sql1)
        cur.execute(sql2)
        cur.execute(sql3)
        con.commit()
        return ""
    except psycopg2.DatabaseError as e:
        debug(7,"error %s enountered in addTempUsers" % e)
        return "error %s enountered in addTempUsers" % e

def addTempUsers2(con,cur):

    pwd1 = "ptest1"
    pwd2 = "ptest2"
    pwd3 = "ptest3"


    sql1 = "insert into  annotationschematest.userstable (username, passwordhash,name,description,isactive) values ('user1', 'puser1', 'user1 name', 'user1 description' , true)"
    sql2 = "insert into  annotationschematest.userstable (username, passwordhash,name,description,isactive) values ('user2', 'puser2', 'user2 name', 'user2 description' , true)"
    sql3 = "insert into  annotationschematest.userstable (username, passwordhash,name,description,isactive) values ('user3', 'puser3', 'user3 name', 'user3 description' , true)"
    try:
        cur.execute(sql1)
        cur.execute(sql2)
        cur.execute(sql3)
        con.commit()
        return ""
    except psycopg2.DatabaseError as e:
        debug(7,"error %s enountered in addTempUsers" % e)
        return "error %s enountered in addTempUsers" % e
"""
