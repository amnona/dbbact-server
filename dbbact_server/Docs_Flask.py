import json

from flask import Blueprint, render_template, request
# from flask.ext.login import current_user

from .utils import debug
from .autodoc import auto


Docs_Flask_Obj = Blueprint('Docs_Flask_Obj', __name__, template_folder='templates')


@Docs_Flask_Obj.route('/get_supported_version', methods=['GET'])
def get_supported_version():
    """
    Title: Get supported client versions
    URL: /docs/get_supported_version
    Method: GET
    URL Params:
    JSON Params:
        {
            "client": str
                Name of the client to check versions for. currently supported:
                "dbbact_calour": the dbbact interface for Calour (https://github.com/amnona/dbbact-calour)
        }
    Success Response:
        Code : 201
        Content :
        {
            "min_version" : float
                The minimal version for fully supported client
            "current_version" : float
                The current version for the client
        }
    """
    debug(1, 'docs/get_supported_version')
    try:
        alldat = request.get_json()
        client = alldat.get('client').lower()
    except Exception as e:
        debug(2, e)
        return json.dumps({'expId': [], 'errorCode': e, 'errorText': e.message})

    # version lists for common clients
    versions = {'dbbact_calour': {'min_version': 1, 'current_version': 2020.0130}}

    if client not in versions:
        return json.dumps('Client %s not in client version list' % client)
    return json.dumps({'min_version': versions[client]['min_version'], 'current_version': versions[client]['current_version']})


@Docs_Flask_Obj.route('/docs', methods=['POST', 'GET'])
def docs():
    '''
    The documentation for all the REST API using flask-autodoc
    '''
    output = '<html>\n<title>dbBact REST API Documentation</title><head>\n</head><body>'
    output += '<h1>dbBact restAPI</h1>'
    output += '<h2>Server location</h2>'
    output += 'The production dbBact restAPI can be accessed at: http://api.dbbact.org<br>'
    output += '<h2>dbBact API commands</h2>'
    output += 'Following is the list of API endpoints '
    output += '(click on an endpoint for details): <br><br>'
    doclist = auto.generate()
    for cdoc in doclist:
        if cdoc is None:
            continue
        output += '<details>\n'
        output += '<summary>'
        output += str(cdoc.get('rule', 'na\n'))
        output += '</summary>\n'
        output += '<pre>\n'
        output += str(cdoc.get('docstring', 'na\n'))
        output += '</pre>\n'
        output += '</details>\n'
    output += '</body>'
    return output
