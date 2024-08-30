from flask import Blueprint
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
    output += 'The production dbBact REST-API can be accessed at: http://api.dbbact.org<br>'
    output += 'Examples for using the REST-API available <a href="https://github.com/amnona/dbbact-examples">here</a><br>'
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

@Docs_Flask_Obj.route('/', methods=['POST', 'GET'])
def welcome():
    # get the version from the __init__.py file
    from . import __version__
    return '<h1>dbBact REST-API server</h1><h2>version %s</h2>\nSee <a href=https://api.dbbact.org/docs>/docs</a> for API documentation.<br>For more details, see the dbBact website (<a href=https://dbbact.org>dbbact.org)</a> or read the <a href=https://doi.org/10.1093/nar/gkad527>paper</a>:<br>Amir, A., Ozel, E., Haberman, Y., & Shental, N. (2023). Achieving pan-microbiome biological insights via the dbBact knowledge base. Nucleic Acids Research, 51(13), 6593-6608.' % __version__
