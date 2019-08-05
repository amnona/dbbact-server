from flask import Blueprint, render_template
# from flask.ext.login import current_user

from .autodoc import auto


Docs_Flask_Obj = Blueprint('Docs_Flask_Obj', __name__, template_folder='templates')


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
