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
