import json

from flask import Blueprint, request, g
from flask_login import current_user
from flask_login import login_required
    
from . import dbannotations
from .utils import debug, getdoc
from .autodoc import auto


Annotation_Flask_Obj = Blueprint('Annottion_Flask_Obj', __name__, template_folder='templates')


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/add', methods=['POST', 'GET'])
def add_annotations():
    """
    Title: Add new annotation
    URL: /annotations/add
    Method: POST
    URL Params:
    Data Params: JSON
        {
            "expId" : int
                (expId from ExperimentsTable)
            "sequences" : list of str (ACGT)
                the sequences to add the annotation to
            "region" : str (optional)
                the name of the primner region where the sequence is from or None for 'na'
                (id from PrimersTable)
            "annotationType" : str
                annotation type (differential expression/contaminant/etc.)
                (description from AnnotationTypesTable)
            "method" : str (optional)
                The method used to detect this behavior (i.e. observation/ranksum/clustering/etc")
                (description from MethodTypesTable)
            "agentType" : str (optional)
                Name of the program which submitted this annotation (i.e. heatsequer)
                (description from AgentTypesTable)
            "description" : str (optional)
                Free text describing this annotation (i.e. "lower in green tomatoes comapred to red ones")
            "private" : bool (optional)
                default=False
                is this annotation private
                private from AnnotationsTable
            "annotationList" : list of
                {
                    "detail" : str
                        the type of detail (i.e. ALL/HIGH/LOW)
                        (description from AnnotationDetailsTypeTable)
                    "term" : str
                        the ontology term_id (i.e. gaz:0004/dbbact:43/envo:0043 etc.) or the ontology term description this detail (i.e. feces/ibd/homo sapiens)
                }
        }
    Success Response:
        Code : 201
        Content :
        {
            "annotationId" : int
            the id from AnnotationsTable for the new annotation.
        }
    Details:
        Validation:
            expId exists in ExperimentsTable
            sequences are all valid ACGT sequences
            region is a valid id from PrimersTable
        Action:
            iterate over sequences, if sequence does not exist, add it to SequencesTable (what to do about taxonomy? - keep it empty?)
            if currType does not exist, add it to AnnotationTypesTable
            if method does not exist, add it to MethodTypesTable
            if agentType does not exist, add it to AgentTypesTable
            iterate over all AnnotationList:
                if detail does not exist, add it to AnnotationsDetailsTypeTable
                if term does not exist, add it to OntologyTable
            Create a new Annotation in AnnotationsTable
            Add all sequence/Annotation pairs to SequenceAnnotationTable
            Add all annotation details to AnnotationsTable (automatically adding userId and addedDate)
            Add all pairs to AnnotationListTable
    """
    debug(3, 'add_annotations', request)
    cfunc = add_annotations
    if request.method == 'GET':
        return(getdoc(cfunc), 400)
    alldat = request.get_json()
    expid = alldat.get('expId')
    if expid is None:
        return(getdoc(cfunc), 400)
    sequences = alldat.get('sequences')
    if sequences is None:
        return(getdoc(cfunc), 400)
    primer = alldat.get('region')
    if primer is None:
        primer = 'na'
    annotationtype = alldat.get('annotationType')
    method = alldat.get('method')
    agenttype = alldat.get('agentType')
    description = alldat.get('description')
    private = alldat.get('private')
    userid = current_user.user_id
    annotationlist = alldat.get('annotationList')
    err, annotationid = dbannotations.AddSequenceAnnotations(g.con, g.cur, sequences, primer, expid, annotationtype, annotationlist, method, description, agenttype, private, userid=userid, commit=True, seq_translate_api=g.seq_translate_api)
    if not err:
        debug(2, 'added sequece annotations')
        return json.dumps({"annotationId": annotationid})
    debug(6, "error encountered %s" % err)
    return ("error enountered %s" % err, 400)


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/update', methods=['POST', 'GET'])
def update_annotations():
    """
    Title: Update existing annotation
    URL: /annotations/update
    Method: POST
    URL Params:
    Data Params: JSON
        {
            "annotationId" : int
                (annotationID from AnnotationsTable)
            "annotationType" : str (optional)
                annotation type (differential expression/contaminant/etc.)
                (description from AnnotationTypesTable).
                or None to not update.
            "method" : str (optional)
                The method used to detect this behavior (i.e. observation/ranksum/clustering/etc")
                (description from MethodTypesTable)
                or None to not update.
            "agentType" : str (optional)
                Name of the program which submitted this annotation (i.e. heatsequer)
                (description from AgentTypesTable)
                or None to not update.
            "description" : str (optional)
                Free text describing this annotation (i.e. "lower in green tomatoes comapred to red ones")
                or None to not update.
            "private" : bool (optional)
                default=False
                is this annotation private
                private from AnnotationsTable
                or None to not update.
            "annotationList" : (optional) list of
                {
                    "detail" : str
                        the type of detail (i.e. ALL/HIGH/LOW)
                        (description from AnnotationDetailsTypeTable)
                    "term" : str
                        the ontology term for this detail (i.e. feces/ibd/homo sapiens)
                        (description from OntologyTable)
                }
                or None to not update.
        }
    Success Response:
        Code : 201
        Content :
        {
            "annotationId" : int
            the id from AnnotationsTable for the updated annotation
        }
    Details:
        Validation:
            annotationID exists in annotations
            User is allowed to modify the annotation (if annotation is annonymous, anyone can update,
            otherwise only the user that created the annotation can update it).
        Action:
            Update all the non-None (supplied) fields in the existing annotation.
    """
    debug(3, 'update_annotations', request)
    cfunc = add_annotations
    if request.method == 'GET':
        return(getdoc(cfunc), 400)
    alldat = request.get_json()
    annotationid = alldat.get('annotationId')
    if annotationid is None:
        return(getdoc(cfunc), 400)
    annotationtype = alldat.get('annotationType')
    method = alldat.get('method')
    agenttype = alldat.get('agentType')
    description = alldat.get('description')
    private = alldat.get('private')
    userid = current_user.user_id
    annotationlist = alldat.get('annotationList')
    err, annotationid = dbannotations.UpdateAnnotation(g.con, g.cur, annotationid=annotationid, annotationtype=annotationtype, annotationdetails=annotationlist, method=method,
                                                       description=description, agenttype=agenttype, private=private, userid=userid,
                                                       commit=True)
    if not err:
        debug(2, 'Updated annotation')
        return json.dumps({"annotationId": annotationid})
    debug(6, "error encountered %s" % err)
    return ("error enountered %s" % err, 400)


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_sequences', methods=['GET'])
def get_annotation_sequences():
    """
    Title: get_sequences
    Description : Get all sequences ids associated with an annotation
    URL: annotations/get_sequences
    Method: GET
    URL Params:
    Data Params: JSON
        {
            annotationid : int
                the annotationid to get the sequences for
        }
    Success Response:
        Code : 200
        Content :
        {
            seqids : list of int
                the seqids for all sequences participating in this annotation
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_annotation_sequences', request)
    cfunc = get_annotation_sequences
    alldat = request.get_json()
    if alldat is None:
        return ('No json parameters supplied', 400)
    annotationid = alldat.get('annotationid')
    if annotationid is None:
        return('annotationid parameter missing', 400)
    err, seqids = dbannotations.GetSequencesFromAnnotationID(g.con, g.cur, annotationid, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    return json.dumps({'seqids': seqids})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_list_sequences', methods=['GET'])
def get_annotation_list_sequences():
    """
    Title: get_list_sequences
    Description : Get all sequences ids associated with a list of annotations
    URL: annotations/get_list_sequences
    Method: GET
    URL Params:
    Data Params: JSON
        {
            annotation_ids : list of int
                list of annotation ids to get the sequences for
        }
    Success Response:
        Code : 200
        Content :
        {
            annotation_seqs : dict of {annotationid (int): list of int (sequence ids)
                the seqids for all sequences participating in each annotation (key)
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_annotation_list_sequences', request)
    cfunc = get_annotation_sequences
    alldat = request.get_json()
    if alldat is None:
        return ('No json parameters supplied', 400)
    annotation_ids = alldat.get('annotation_ids')
    if annotation_ids is None:
        return('annotation_ids parameter missing', 400)
    annotation_seqs = {}
    for cannotation_id in annotation_ids:
        err, seqids = dbannotations.GetSequencesFromAnnotationID(g.con, g.cur, cannotation_id, userid=current_user.user_id)
        if err:
            debug(6, err)
            return ('Problem geting details. error=%s' % err, 400)
        annotation_seqs[cannotation_id] = seqids
    return json.dumps({'annotation_seqs': annotation_seqs})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_full_sequences', methods=['GET'])
def get_annotation_full_sequences():
    """
    Title: get_full_sequences
    Description : Get all the sequences (ACGT) associated with an annotation
    URL: annotations/get_full_sequences
    Method: GET
    URL Params:
    Data Params: JSON
        {
            annotationid : int
                the annotationid to get the sequences for
        }
    Success Response:
        Code : 200
        Content :
        {
            'sequences' : list of dict
                information about each sequence in the annotation
                {
                    'seq' : str (ACGT)
                        the sequence
                    'taxonomy' : str
                        the taxonomy of the sequence (or '' if not present)
                }
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_annotation_full_sequences', request)
    cfunc = get_annotation_full_sequences
    alldat = request.get_json()
    if alldat is None:
        return('No json parameters supplied', 400)
    annotationid = alldat.get('annotationid')
    if annotationid is None:
        return('annotationid parameter missing', 400)
    err, sequences = dbannotations.GetFullSequencesFromAnnotationID(g.con, g.cur, annotationid, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    return json.dumps({'sequences': sequences})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/delete', methods=['GET', 'POST'])
def delete_annotation():
    """
    Title: delete
    Description : Delete annotation
    URL: annotations/delete
    Method: POST
    URL Params:
    Data Params: JSON
        {
            annotationid : int
                the annotationid to delete
        }
    Success Response:
        Code : 200
        Content :
        {
            annotationid : int
                the annotationid deleted
        }
    Details :
        Validation:
            If user is not logged in, cannot delete
            Can only delete annotations created by the user
    """
    debug(3, 'delete_annotation', request)
    cfunc = delete_annotation
    if request.method == 'GET':
        return(getdoc(cfunc), 400)
    alldat = request.get_json()
    if alldat is None:
        return ('No json parameters supplied', 400)
    annotationid = alldat.get('annotationid')
    if annotationid is None:
        return('annotationid parameter missing', 400)
    err = dbannotations.DeleteAnnotation(g.con, g.cur, annotationid, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem deleting annotation. error=%s' % err, 400)
    return json.dumps({'annotationid': annotationid})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/delete_sequences_from_annotation', methods=['GET', 'POST'])
def delete_sequences_from_annotation():
    """
    Title: Delete sequences from annotation
    Description : Delete sequences from an existing annotation
    URL: annotations/delete_sequences_from_annotation
    Method: POST
    URL Params:
    Data Params: JSON
        {
            annotationid : int
                the annotationid to delete
            sequences : list of str ('ACGT')
                the sequences to delete from the annotation
        }
    Success Response:
        Code : 200
        Content :
        {
            annotationid : int
                the annotationid from which the sequences were deleted
        }
    Details :
        Validation:
            If user is not logged in, cannot delete non-annonymous
            Can only delete annotations created by the user
    """
    debug(3, 'delete_sequences_from_annotation', request)
    cfunc = delete_sequences_from_annotation
    if request.method == 'GET':
        return(getdoc(cfunc), 400)
    alldat = request.get_json()
    if alldat is None:
        return ('No json parameters supplied', 400)
    annotationid = alldat.get('annotationid')
    if annotationid is None:
        return('annotationid parameter missing', 400)
    sequences = alldat.get('sequences')
    if sequences is None:
        return('sequences parameter missing', 400)
    err = dbannotations.DeleteSequenceFromAnnotation(g.con, g.cur, sequences, annotationid, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem deleting sequences. error=%s' % err, 400)
    return json.dumps({'annotationid': annotationid})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_annotation', methods=['GET'])
def get_annotation():
    """
    Title: get_annotation
    Description : Get the information about an annotation (based on annotationid)
    URL: annotations/get_annotation
    Method: GET
    URL Params:
    Data Params: Parameters or json
        {
            annotationid : int
                the annotationid to get the details for
        }
    Success Response:
        Code : 200
        Content :
        {
            "annotationid" : int
                the id of the annotation
            "userid" : int
                The user id
                (id from UsersTable)
            "username" : str
                name of the user who added this annotation
                (userName from UsersTable)
            "date" : str (DD-MM-YYYY HH:MM:SS)
                date when the annotation was added
                (addedDate from CurationsTable)
            "expid" : int
                the ID of the experiment from which this annotation originated
                (uniqueId from ExperimentsTable)
                (see Query Experiment)
            "annotationtype" : str
                annotation type (differential expression/contaminant/etc.)
                (can be 'dominant'/'common'/'diffexp'/'isa'/'contamination'/'other'/'positive association'/'negative association')
                (description from CurationTypesTable)
            "method" : str
                The method used to detect this behavior (i.e. observation/ranksum/clustering/etc")
                (description from MethodTypesTable)
            "agent" : str
                Name of the program which submitted this annotation (i.e. heatsequer)
                (description from AgentTypesTable)
            "description" : str
                Free text describing this annotation (i.e. "lower in green tomatoes comapred to red ones")
            "private" : bool
                True if the curation is private, False if not
            "num_sequences" : int
                The number of sequences associated with this annotation
            "flags": list of dict {'flagid': int, status:str, userid: int}
                the flags raised for this annotation by other users (if not empty, maybe should suspect this annotation)
            'primerid': int
                id of the primer region of the annotation
            'primer': str
                name of the primer region of the annotation (i.e. 'v4')
            "details" : list of
                {
                    "detail" : str
                        the type of detail (i.e. ALL/HIGH/LOW)
                        (description from CurationDetailsTypeTable)
                    "term" : str
                        the ontology term for this detail (i.e. feces/ibd/homo sapiens)
                        (description from OntologyTable)
                }
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_annotation', request)
    cfunc = get_annotation
    annotationid = request.args.get('annotationid')
    if annotationid is None:
        alldat = request.get_json()
        if alldat is None:
            return('No parameters supplied', 400)
        annotationid = alldat.get('annotationid')
        if annotationid is None:
            return(getdoc(cfunc))
    annotationid = int(annotationid)
    err, annotation = dbannotations.GetAnnotationsFromID_prep(g.con, g.cur, annotationid)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    return json.dumps(annotation)


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_annotation_ontology_parents', methods=['GET', 'POST'])
def get_annotation_ontology_parents():
    """
    Title: get_annotation_ontology_parents
    Description : Get all the ontology terms (and their parents in the ontolgy DAG) for the annotation
    URL: annotations/get_annotation_ontology_parents
    Method: GET
    URL Params:
    Data Params: JSON
        {
            annotationid : int
                the annotationid to get the ontology parents for
            get_term_id: bool, optional (default=False)
                True will get the parent term_ids (i.e. 'gaz:000001')
                False will get the parent terms (i.e. 'saliva')
        }
    Success Response:
        Code : 200
        Content :
        {
            parents : dict of (str:list of str) (detail type (i.e. 'low'/'high'/'all'), list of ontology terms)
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_annotation_ontology_parents', request)
    cfunc = get_annotation_ontology_parents
    alldat = request.get_json()
    if alldat is None:
        return ('No json parameters supplied', 400)
    annotationid = alldat.get('annotationid')
    get_term_id = alldat.get('get_term_id', False)
    # annotationid=int(request.args.get('annotationid'))
    if annotationid is None:
        return(getdoc(cfunc))
    err, parents = dbannotations.GetAnnotationParents(g.con, g.cur, annotationid, get_term_id=get_term_id)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    return json.dumps({'parents': parents})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_all_annotations', methods=['GET'])
def get_all_annotations():
    """
    Title: get_all_annotations
    Description : Get list of all annotations in dbBact
    URL: annotations/get_all_annotations
    Method: GET
    URL Params:
    Data Params: JSON
        {
        }
    Success Response:
        Code : 200
        Content :
        {
            annotations : list of annotation
            See annotations/get_annotation() for details
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_all_annotations', request)
    err, annotations = dbannotations.GetAllAnnotations(g.con, g.cur, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem geting all annotations list. error=%s' % err, 400)
    return json.dumps({'annotations': annotations})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/add_annotation_flag', methods=['POST'])
def add_annotation_flag():
    """
    Title: add_annotation_flag
    Description : Flag an annotation as suspicious (i.e. wrong sequenes/region/terms/seems incorrect)
    URL: annotations/add_annotation_flag
    Method: POST
    URL Params:
    Data Params: JSON
        {
        annotationid: int
            The annotationID of the annotation to flag
        reason: str
            The reason to flag this annotation (as many details as possible)
        }
    Success Response:
        Code : 200
        Content :
        {
        }
    Details :
        Validation:
            Must be a registered user (cannot flag as annonimous)
            Must supply a reason
            Once an annotation is flagged, the flag status is "suggested". The flag will be reviewed by the dbbact team and then the status
            changed to "accepted" or "rejected"
    """
    debug(3, 'add_annotation_flag', request)
    cfunc = add_annotation_flag
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc), 400)
    # if current_user.user_id == 0:
    #     return('Cannot add flag as annonymous user. Please register and supply user/password in the request', 400)
    reason = alldat.get('reason')
    if reason is None:
        return(getdoc(cfunc), 400)
    if reason == '':
        return('Must supply reason for flagging the annotation', 400)
    annotationid = alldat.get('annotationid')
    err = dbannotations.add_annotation_flag(g.con, g.cur, annotationid=annotationid, userid=current_user.user_id, reason=reason)
    if err:
        debug(6, err)
        return ('Problem flagging annotation. error=%s' % err, 400)
    return 'added flag to annotation'


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/get_annotation_flags', methods=['GET'])
def get_annotation_flags():
    """
    Title: get_annotation_flags
    Description : Get flags associated with an annotation
    URL: annotations/get_annotation_flags
    Method: GET
    URL Params:
    Data Params: JSON
        {
        annotationid: int
            The annotationID of the annotation to get the flag
        }
    Success Response:
        Code : 200
        Content :
        {
            flags: list of dict {'flagid': int, status:str, userid: int}
        }
    Details :
        Validation:
    """
    debug(3, 'get_annotation_flags', request)
    cfunc = get_annotation_flags
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc), 400)
    annotationid = alldat.get('annotationid')
    err, flags = dbannotations.get_annotation_flags(g.con, g.cur, annotaitonid=annotationid)
    if err:
        debug(6, err)
        return ('Problem getting annotation flags. error=%s' % err, 400)
    return json.dumps({'flags': flags})


@login_required
@auto.doc()
@Annotation_Flask_Obj.route('/annotations/delete_annotation_flag', methods=['POST'])
def delete_annotation_flag():
    """
    Title: delete_annotation_flag
    Description : Delete an annotation flag (only if flag was created by the same user)
    URL: annotations/delete_annotation_flag
    Method: POST
    URL Params:
    Data Params: JSON
        {
        flagid: int
            The id of the annotation flag to delete
        }
    Success Response:
        Code : 200
        Content :
        {
            flags: list of dict {'flagid': int, status:str, userid: int}
        }
    Details :
        Validation:
        Can only delete if the user that created the flag is the user requesting delete
    """
    debug(3, 'delete_annotation_flag', request)
    cfunc = delete_annotation_flag
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc), 400)
    flagid = alldat.get('flagid')
    err = dbannotations.delete_annotation_flag(g.con, g.cur, flagid=flagid, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem deleting annotation. error=%s' % err, 400)
    return 'flag %d deleted' % flagid
