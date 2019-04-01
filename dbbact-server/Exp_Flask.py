import json
from flask import Blueprint, g, request
from flask_login import current_user

from .utils import getdoc, debug
from .autodoc import auto
from . import dbexperiments
from . import dbannotations


Exp_Flask_Obj = Blueprint('Exp_Flask_Obj', __name__)


@Exp_Flask_Obj.route('/experiments/add_details', methods=['GET', 'POST'])
@auto.doc()
def add_details():
    """
    Title: Add new experiment detail
    URL: /AddExpDetails/
    Method: POST
    URL Params:
    Data Params:
        {
            "expId" : int
                (expId from ExperimentsTable) or 0 to add a new experiment
            "private" : bool (optional)
                    default=False
                    whether the experiment should be private or public (default)
                    (private from Experiments)
            "details" : list of
            {
                "type" : str
                    the detail type (i.e. "pubmedid"/"author"/etc.)
                    (description from ExperimentTypesTable linked from ExperimentsIdentifiers)
                "value" : str
                    the detail value (i.e. "john smith"/"pmid003344"/etc.)
                    (value from ExperimentsIdentifiers)
            }
        }
    Success Response:
        Code : 201
        Content :
        {
            "expId" : int
                the expId for the experiment with the details added
                same as the data expId if it was >0
                or the expId for the new experiment created
        }
    Details:
        Validation:
            if expId>0, make sure the experiment exists
            If the detail already exists for the experiment, update it?
        Action:
            If ExpId not supplied, get a new unique experimentId (can use the current uniqueid in ExperimentsTable) and add the private field and the userID/Date
            Add entries into ExperimentsTable for all the pairs in the list. For each one, automatically add the userId and date
            Return the new expId for these details
            for each "type"/"value" in the "details" list, if "type" exists in ExperimentTypesTable, get the id and add it to "type" field in ExperimentsIdentifiers table. Otherwise, create it there and get the id and add it to "type" field in ExperimentsIdentifiers.
    """
    debug(3, 'experiments/add_details', request)
    cfunc = add_details
    if request.method == 'GET':
        return(getdoc(cfunc))
    alldat = request.get_json()
    expid = alldat.get('expId')
    details = alldat.get('details')
    if details is None:
        return(getdoc(cfunc))
    private = alldat.get('private')
    if private is None:
        private = 'n'
    if expid == -1:
        expid = None
    # TODO: get userid
    userid = 0
    res = dbexperiments.AddExperimentDetails(g.con, g.cur, expid=expid, details=details, userid=userid, private=private, commit=True)
    if res > 0:
        return(json.dumps({'expId': res}))
    if res == -1:
        return('expId %d does not exist' % expid, 400)
    return("AddExperimentDetails failed", 400)


@Exp_Flask_Obj.route('/experiments/get_id_by_list', methods=['GET'])
@auto.doc()
def get_id_by_list():
    """
    Title: Query experiment based on list of type/value:
    Description: Get IDs of all experiments matching one of the fields in field/value pairs (i.e. "Pubmedid","111222")
    URL: /experimets/get_id_by_list
    Method: GET
    URL Params: JSON
        {
            details : list of tuples [type,value] of str where:
                type : str
                    the detail type (i.e. "pubmedid"/"author"/etc.)
                    (type from ExperimentsTable)
                value : str
                    the detail value (i.e. "john smith"/"pmid003344"/etc.)
        }
    Success Response:
        Code : 200
        Content :
        {
            "experiment" : id
            {
                expId : int
                    the expId for an experiment matching the query
            }
        }
    Details :
        Validation:
            If study is private, return only if user is authenticated and created the study. If user not authenticated, do not return it in the list
            If study is not private, return it (no need for authentication)
    """
    debug(3, 'experiments/get_id_by_list', request)
    alldat = request.get_json()
    nameArr = alldat.get('nameStrArr')
    valueArr = alldat.get('valueStrArr')

    if (nameArr is None) or (valueArr is None):
        return('no details')
    # TODO: get userid
    userid = 0
    err, expId = dbexperiments.GetExperimentIdByVals(g.con, g.cur, nameArr, valueArr, userid)
    if not err:
        return json.dumps({'expId': expId, 'errorCode': 0, 'errorText': ''})
    else:
        return json.dumps({'expId': expId, 'errorCode': expId, 'errorText': err})


@Exp_Flask_Obj.route('/experiments/get_id', methods=['GET'])
@auto.doc()
def get_id():
    """
    Title: Query experiment based on type/value:
    Description: Get IDs of all experiments matching a field/value pair (i.e. "Pubmedid","111222")
    URL: /experimets/get_id
    Method: GET
    URL Params: JSON
        {
            details : list of tuples [type,value] of str where:
                type : str
                    the detail type (i.e. "pubmedid"/"author"/etc.)
                    (type from ExperimentsTable)
                value : str
                    the detail value (i.e. "john smith"/"pmid003344"/etc.)
        }
    Success Response:
        Code : 200
        Content :
        {
            "experiments" : list of
            {
                expId : int
                    the expId for an experiment matching the query
            }
        }
    Details :
        Validation:
            If study is private, return only if user is authenticated and created the study. If user not authenticated, do not return it in the list
            If study is not private, return it (no need for authentication)
    """
    debug(3, 'experiments/get_id', request)
    alldat = request.get_json()
    details = alldat.get('details')
    if details is None:
        return('no details')
    # TODO: get userid
    userid = 0
    err, cids = dbexperiments.GetExperimentId(g.con, g.cur, details, userid)
    if not err:
        return json.dumps({'expId': cids})
    else:
        return (err, 400)


@Exp_Flask_Obj.route('/experiments/get_details', methods=['GET'])
@auto.doc()
def get_details():
    """
    Title: Query experiment based on experiment id
    Description: Get the details (i.e. 'pubmedid':'1665344' etc.) of experiment with a given id
    URL: /experimets/get_data
    Method: GET
    URL Params: JSON
        {
            "expId" : int
                the experiment id
        }
    Success Response:
        Code : 200
        Content :
        {
            "details" : list of
            {
                type : str
                    the field name (i.e. "pubmedid")
                value : str
                    the value of the field (i.e. "1665344")
            }
        }
    Details :
        Validation:
            If study is private, return only if user is authenticated and created the study. If user not authenticated, return experiment not found
            If study is not private, return details (no need for authentication)
            if study not found - return error
    """
    debug(3, 'experiments/get_details', request)
    alldat = request.get_json()
    if alldat is None:
        return('no expId supplied', 400)
    expid = alldat.get('expId')
    if expid is None:
        return('no expId supplied', 400)
    # TODO: get userid
    userid = 0
    err, details = dbexperiments.GetDetailsFromExpId(g.con, g.cur, expid, userid)
    if err:
        return(err, 400)
    return json.dumps({'details': details})


@Exp_Flask_Obj.route('/experiments/get_annotations', methods=['GET'])
@auto.doc()
def get_annotations():
    """
    Title: Query annotations based on experiment id
    Description: Get the annotations associated with an experiment
    URL: /experimets/get_annotations
    Method: GET
    URL Params: JSON
        {
            "expId" : int
                the experiment id
        }
    Success Response:
        Code : 200
        Content :
        {
            "annotations" : list of dict:
            {
                "userid" : int
                "annotationtype" : str
                "method" : str
                "data" : str
                "agent" : str
                "description" : str
                "private" : str
            }
        }
    Details :
        Validation:
            If study is private, return only if user is authenticated and created the study. If user not authenticated, return experiment not found
            if annotation is private, return only if created by the same user as the querying
            if study not found - return error
    """
    debug(3, 'experiments/get_annotations', request)
    alldat = request.get_json()
    if alldat is None:
        return('no expId supplied', 400)
    expid = alldat.get('expId')
    if expid is None:
        return('no expId supplied', 400)
    # TODO: get userid
    userid = 0
    err, annotations = dbannotations.GetAnnotationsFromExpId(g.con, g.cur, expid, userid)
    if err:
        return(err, 400)
    return json.dumps({'annotations': annotations})


@Exp_Flask_Obj.route('/experiments/get_experiments_list', methods=['GET'])
@auto.doc()
def get_experiments_list():
    """
    Title: get_experiments_list
    Description: Get list of all experiments in the database and their details
    URL: /experimets/get_experiments_list
    Method: GET
    URL Params: JSON
        {
        }
    Success Response:
        Code : 200
        Content :
        {
            "explist" : list of tuples:
            {
                expid : int
                details : tuples of (name, value)
            }
        }
    Details :
        Validation:
            If experiment is private, return only if user is authenticated and created the study.
    """
    debug(3, 'experiments/get_experiments_list', request)
    err, expdat = dbexperiments.GetExperimentsList(g.con, g.cur, userid=current_user.user_id)
    if err:
        return(err, 400)
    return json.dumps({'explist': expdat})
