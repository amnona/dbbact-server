import json
from flask import Blueprint, g, request
from .utils import debug
from .autodoc import auto
from . import dbstats


DBStats_Flask_Obj = Blueprint('DBStats_Flask_Obj', __name__, template_folder='templates')


@DBStats_Flask_Obj.route('/stats/stats', methods=['GET'])
@auto.doc()
def dbdstats():
    """
    Title: Get statistics about the database
    URL: /stats/stats
    Method: GET
    URL Params:
    Data Params:
     Success Response:
        Code : 201
        Content :
        stats : dict
        {
            "NumSequences" : int
                number of unique sequences in the sequenceTable (each sequence can appear in multiple annotations)
            "NumAnnotations" : int
                number of unique annotations (each annotation contains many sequences)
            "NumSeqAnnotations" : int
                number of sequence annotations in the sequenceTable
            "NumOntologyTerms" : int
                number of ontology terms in the OntologyTable
            "NumExperiments" : int
                number of unique expIDs in the ExperimentsTable
            "Database": str
                name of current database for which the stats are returned
        }
    Details:
    """
    debug(3, 'dbstats', request)
    err, stats = dbstats.GetStats(g.con, g.cur)
    if not err:
        debug(2, 'Got statistics')
        return json.dumps({'stats': stats})
    errmsg = "error encountered %s" % err
    debug(6, errmsg)
    return (errmsg, 400)


@DBStats_Flask_Obj.route('/stats/get_supported_version', methods=['GET'])
@auto.doc()
def get_supported_version():
    """
    Title: Get supported client versions
    URL: /stats/get_supported_version
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
            "min_version" : str
                The minimal version for fully supported client
            "current_version" : str
                The current version for the client
        }
    """
    # version lists for common clients
    versions = {'dbbact_calour': {'min_version': '1.0.0', 'current_version': '1.0.1'}}

    debug(1, 'docs/get_supported_version')
    try:
        alldat = request.get_json()
        client = alldat.get('client').lower()
    except Exception as e:
        debug(2, e)
        return json.dumps(versions)

    if client not in versions:
        return json.dumps('Client %s not in client version list' % client)
    return json.dumps({'min_version': versions[client]['min_version'], 'current_version': versions[client]['current_version']})
