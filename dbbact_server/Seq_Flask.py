import json
from flask import Blueprint, request, g
from flask_login import login_required, current_user
from . import dbsequences
from . import dbannotations
from . import dbontology
from . import dbprimers
from .utils import debug, getdoc
from .autodoc import auto
# NOTE: local flask_cors module, not pip installed!
# from flask_cors import crossdomain

Seq_Flask_Obj = Blueprint('Seq_Flask_Obj', __name__, template_folder='templates')
# CORS(Seq_Flask_Obj)


@Seq_Flask_Obj.route('/sequences/add', methods=['POST', 'GET'])
@auto.doc()
def add_sequences():
    """
    Title: Add new sequences (or return seqid for ones that exist)
    URL: /sequences/add
    Method: POST
    URL Params:
    Data Params: JSON
        {
            "sequences" : list of str
                the sequences to add (acgt)
            "taxonomies" : list of str (optional)
                the taxonomy per sequence (if not provided, na will be used)
            "ggids" : list of int (optional)
                list of GreenGenes id per sample (if not provided, 0 will be used)
            "primer" : str
                name of the primer region (i.e. 'V4'). if region does not exist, will fail
        }
    Success Response:
        Code : 201
        Content :
        {
            "seqIds" : list of int
                the new sequence id per sequence in the list
        }
    Details:
        Validation:
        Action:
        Add all sequences that don't already exist in SequencesTable
    """
    debug(3, 'add_sequences', request)
    cfunc = add_sequences
    if request.method == 'GET':
        return(getdoc(cfunc))
    alldat = request.get_json()
    sequences = alldat.get('sequences')
    if sequences is None:
        return(getdoc(cfunc))
    taxonomies = alldat.get('taxonomies')
    ggids = alldat.get('ggids')
    primer = alldat.get('primer')
    if primer is None:
        return(getdoc(cfunc))

    err, seqids = dbsequences.AddSequences(g.con, g.cur, sequences=sequences, taxonomies=taxonomies, ggids=ggids, primer=primer, seq_translate_api=g.seq_translate_api)
    if err:
        return(err, 400)
    debug(2, 'added/found %d sequences' % len(seqids))
    return json.dumps({"seqIds": seqids})


@Seq_Flask_Obj.route('/sequences/getid', methods=['GET'])
@auto.doc()
def get_sequenceid():
    """
    Title: Get id for a given sequence (or return -1 if does not exist)
    URL: /sequences/getid
    Method: GET
    URL Params:
    Data Params: JSON
        {
            "sequence" : str
                the sequence to get data about. could be 'acgt' or SILVA ID (in case dbname is set to 'silva')
            "no_shorter" : bool (optional)
                False (default) to get also shorter sequences from DB if matching.
                True to get only sequences at least as long as the query
            "no_longer" : bool (optional)
                False (default) to get also longer sequences from DB if matching on query length.
                True to get only sequences not longer than the query
            "use_sequence_translator": bool (optional)
                True to get also annotations for dbbact sequences from other regions linked to the query sequences using the wholeseqdb (i,e, SILVA)
                False (default) to get just annotations for dbbact sequences that match exactly the queryy sequences
            "dbname": str, optional
                If supplied (i.e. 'silva'), assume sequence is the identifier in dbname (i.e.  'FJ978486' for 'silva' instead of acgt sequence)
        }
    Success Response:
        Code : 201
        Content :
        {
            "seqId" : list of int
                the sequence ids, or []] if doesn't exists
                Note: can be more than 1 id since may contain subsequences (if no_longer is False or no_shorter is False) or different regions (if use_sequence_translator=True)
        }
    Details:
        Validation:
        Action:
    """
    debug(3, 'get_sequenceid', request)
    cfunc = get_sequenceid
    alldat = request.get_json()
    sequence = alldat.get('sequence')
    no_shorter = alldat.get('no_shorter', False)
    no_longer = alldat.get('no_longer', False)
    use_sequence_translator = alldat.get('use_sequence_translator', False)
    dbname = alldat.get('dbname', None)
    if dbname is not None:
        use_sequence_translator = True
    if use_sequence_translator:
        seq_translate_api = g.seq_translate_api
    else:
        seq_translate_api = None
    if sequence is None:
        return(getdoc(cfunc))

    err, seqid = dbsequences.GetSequenceId(g.con, g.cur, sequence=sequence, no_shorter=no_shorter, no_longer=no_longer, seq_translate_api=seq_translate_api, dbname=dbname)
    if err:
        return(err, 400)
    debug(2, 'found sequences')
    return json.dumps({"seqId": seqid})


@Seq_Flask_Obj.route('/sequences/getid_list', methods=['GET'])
@auto.doc()
def get_sequenceid_list():
    """
    Title: Get dbbact ids for a list of given sequences
    URL: /sequences/getid_list
    Method: GET
    URL Params:
    Data Params: JSON
        {
            "sequences" : list of str
                the list of sequences to get data about
            "no_shorter" : bool (optional)
                False (default) to get also shorter sequences from DB if matching.
                True to get only sequences at least as long as the query
            "no_longer" : bool (optional)
                False (default) to get also longer sequences from DB if matching on query length.
                True to get only sequences not longer than the query
            "use_sequence_translator": bool (optional)
                True to get also annotations for dbbact sequences from other regions linked to the query sequences using the wholeseqdb (i,e, SILVA)
                False (default) to get just annotations for dbbact sequences that match exactly the queryy sequences
            "dbname": str, optional
                If supplied (i.e. 'silva'), assume sequence is the identifier in dbname (i.e.  'FJ978486' for 'silva' instead of acgt sequence)
        }
    Success Response:
        Code : 201
        Content :
        {
            "seqIds" : list of list of int
                the sequence ids matching each original sequence,
                Note: can be more than 1 id per sequence since we are looking for subsequences
        }
    Details:
        Validation:
        Action:
    """
    cfunc = get_sequenceid_list
    alldat = request.get_json()
    sequences = alldat.get('sequences')
    no_shorter = alldat.get('no_shorter', False)
    no_longer = alldat.get('no_longer', False)
    dbname = alldat.get('dbname', None)
    if sequences is None:
        return(getdoc(cfunc))
    use_sequence_translator = alldat.get('use_sequence_translator', False)
    if dbname is not None:
        use_sequence_translator = True
    if use_sequence_translator:
        seq_translate_api = g.seq_translate_api
    else:
        seq_translate_api = None

    out_list = []
    for cseq in sequences:
        err, seqid = dbsequences.GetSequenceId(g.con, g.cur, sequence=cseq, no_shorter=no_shorter, no_longer=no_longer, seq_translate_api=seq_translate_api, dbname=dbname)
        if err:
            debug(4, 'Sequence %s not found from get_sequenceid_list' % cseq)
            seqid = []
            # return(err, 400)
        out_list.append(seqid)
    debug(2, 'found sequences')
    return json.dumps({"seqIds": out_list})


@login_required
@Seq_Flask_Obj.route('/sequences/get_taxonomy_str', methods=['GET'])
@auto.doc()
def get_taxonomy_str():
    """
    Title: Query sequence:
    Description : Get the dbbact stored taxonomy about a given sequence
    URL: /sequences/get_taxonomy_str
    Method: GET
    URL Params:
    Data Params: JSON
        {
            sequence : str
                the DNA sequence string to query the database (can be any length)
            region : int (optional)
                the region id (default=1 which is V4 515F 806R)
    Success Response:
        Code : 200
        Content :
        {
            "taxonomy" : str
        }
    """
    debug(3, 'get_taxonomy_str', request)
    cfunc = get_taxonomy_str
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    sequence = alldat.get('sequence')
    if sequence is None:
        return('sequence parameter missing', 400)

    err, taxonomyStr = dbsequences.GetSequenceTaxonomy(g.con, g.cur, sequence, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    return json.dumps({'taxonomy': taxonomyStr})


@login_required
@Seq_Flask_Obj.route('/sequences/get_annotations', methods=['GET'])
@auto.doc()
def get_sequence_annotations():
    """
    Title: Query sequence:
    Description : Get all the annotations about a given sequence
    URL: /sequences/get_annotations
    Method: GET
    URL Params:
    Data Params: JSON
        {
            "sequence" : str
                the DNA sequence string to query the database (can be any length). Can also be a SILVA ID if dbname is set to "silva"
            "region" : int (optional)
                the region id (default=1 which is V4 515F 806R)
            "get_term_info" : bool (optional)
                True (default) to get information about all ontology predecessors of terms of all annotations of the sequence.
            "get_tax_info": book (optional)
                True (default) to get the dbbact taxonomy string of the sequence (or None if not in dbbact)
            "use_sequence_translator": bool (optional)
                True (default) to get also annotations for dbbact sequences from other regions linked to the query sequences using the wholeseqdb (i,e, SILVA)
                False to get just annotations for dbbact sequences that match exactly the queryy sequences
            "dbname": str, optional
                If supplied (i.e. 'silva'), assume sequence is the identifier in dbname (i.e.  'FJ978486' for 'silva' instead of acgt sequence)
    Success Response:
        Code : 200
        Content :
        {
            "taxonomy" : str
                the taxonomy from dbBact taxonomies (if availble).
                Not returned if get_tax_info is False
            "annotations" : list of
                {
                    "annotationid" : int
                        the id of the annotation
                    "userid" : int
                        The user id
                        (id from UsersTable)
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
            term_info : dict of {term: dict}
            Information about all ontology terms associated with any of the annotations (including predecessors)
                key: term (str)
                value: dict of pairs:
                    'total_annotations' : number of annotations having this term in the database (int)
                    'total_sequences' : number of sequences in annotations having this term in the database (int)
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_sequence_annotations', request)
    cfunc = get_sequence_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    sequence = alldat.get('sequence')
    if sequence is None:
        return('sequence parameter missing', 400)
    get_term_info = alldat.get('get_term_info', True)
    get_tax_info = alldat.get('get_tax_info', True)
    region = alldat.get('region')
    use_sequence_translator = alldat.get('use_sequence_translator', True)
    dbname = alldat.get('dbname', None)
    if dbname is not None:
        use_sequence_translator = True

    taxonomy = None
    if get_tax_info:
        err, taxonomy = dbsequences.GetSequenceTaxonomy(g.con, g.cur, sequence, userid=current_user.user_id)
        if err:
            taxonomy = 'error: err'
    if use_sequence_translator:
        seq_translate_api = g.seq_translate_api
    else:
        seq_translate_api = None

    err, details = dbannotations.GetSequenceAnnotations(g.con, g.cur, sequence, userid=current_user.user_id, region=region, seq_translate_api=seq_translate_api, dbname=dbname)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    if get_term_info:
        term_info = dbontology.get_annotations_term_counts(g.con, g.cur, details)
    else:
        term_info = {}
    return json.dumps({'annotations': details, 'term_info': term_info, 'taxonomy': taxonomy})


@login_required
@Seq_Flask_Obj.route('/sequences/get_list_annotations', methods=['GET'])
@auto.doc()
def get_sequence_list_annotations():
    """
    Title: Query sequence:
    Description : Get all the annotations about a list of sequences
    URL: /sequences/get_annotations
    Method: GET
    URL Params:
    Data Params: JSON
        {
            "sequences": list of str ('ACGT')
                the list of sequence strings to query the database (can be any length), or alternatively silva IDs (if dbname='silva')
            "region": int (optional)
                the region id (default=1 which is V4 515F 806R)
            "use_sequence_translator": bool (optional)
                True (default) to get also annotations for dbbact sequences from other regions linked to the query sequences using the wholeseqdb (i,e, SILVA)
                False to get just annotations for dbbact sequences that match exactly the queryy sequences
            "dbname": str, optional
                If supplied (i.e. 'silva'), assume sequence is the identifier in dbname (i.e.  'FJ978486' for 'silva' instead of acgt sequence)
    Success Response:
        Code : 200
        Content :
        {
            'seqannotations': list
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
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_list_annotations', request)
    cfunc = get_sequence_list_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    sequences = alldat.get('sequences')
    if sequences is None:
        return('sequences parameter missing', 400)
    use_sequence_translator = alldat.get('use_sequence_translator', True)
    dbname = alldat.get('dbname', None)
    if dbname is not None:
        use_sequence_translator = True
    if use_sequence_translator:
        seq_translate_api = g.seq_translate_api
    else:
        seq_translate_api = None

    seqannotations = []
    for cseq in sequences:
        err, details = dbannotations.GetSequenceAnnotations(g.con, g.cur, cseq, userid=current_user.user_id, seq_translate_api=seq_translate_api, dbname=dbname)
        # if err:
        #   debug(6,err)
        #   return ('Problem geting details. error=%s' % err,400)
        seqannotations.append(details)

    return json.dumps({'seqannotations': seqannotations})


# # need to add conversion to nice string
# @login_required
# @Seq_Flask_Obj.route('/sequences/get_annotations_string', methods=['GET'])
# @auto.doc()
# def get_annotations_string():
#     cfunc = get_annotations_string
#     alldat = request.get_json()
#     if alldat is None:
#         return(getdoc(cfunc))
#     sequence = alldat.get('sequence')
#     if sequence is None:
#         return('sequence parameter missing', 400)
#     err, details = dbannotations.GetSequenceAnnotations(g.con, g.cur, sequence, userid=current_user.user_id)
#     if err:
#         debug(6, err)
#         return ('Problem geting details. error=%s' % err, 400)
#     return json.dumps({'annotations': details})


@login_required
@Seq_Flask_Obj.route('/sequences/get_fast_annotations', methods=['GET'])
@auto.doc()
def get_fast_annotations():
    """
    Title: Get Fast Annotations
    Description : Get annotations for a list of sequences in a compressed form
    URL: /sequences/get_fast_annotations
    Method: GET
    URL Params:
    Data Params: JSON
        {
            "sequences": list of str ('ACGT')
                the list of sequence strings to query the database (can be any length). Alternatively, can be list of SILVA IDs (in case dbname is set to 'silva')
            "region": int (optional)
                the region id (default=1 which is V4 515F 806R)
            "get_term_info": bool (optional)
                True (default) to return also information about each term, False not to return
            "get_taxonomy": bool (optional)
                True (default) to get the dbbact assigned taxonomy for each query sequence
            "get_parents": bool (optional)
                True (default) to get the parent terms for each annotation ontology term, False to just get tge annotation terms
            "get_all_exp_annotations": bool (optional)
                True (default) to get all the annotations from each experiment containing one annotation with the sequence, False to just get the annotations with the sequence
            "use_sequence_translator": bool (optional)
                True (default) to get also annotations for dbbact sequences from other regions linked to the query sequences using the wholeseqdb (i,e, SILVA)
                False to get just annotations for dbbact sequences that match exactly the queryy sequences
            "dbname": str, optional
                If supplied (i.e. 'silva'), assume sequence is the identifier in dbname (i.e.  'FJ978486' for 'silva' instead of acgt sequence)
    Success Response:
        Code : 200
        Content :
        {
            annotations: dict of (annotationid: details):
                    annotationid : the annotationid used in seqannotations
                    details:
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
                    "parents" : list of tuples (type, list of terms)
                        {
                            type : type of the annotation type ('high'/'low','all')
                            list of terms - list of ontology terms which are annotated or parents of annotated ontology term
                        }
                }
            seqannotations : list of (seqid, annotationids):
            {
                    seqpos : position of the sequence in the list
                    annotationids : list of int
                            the annotationsid associated with this sequence
            }
            term_info : dict of {term, dict}:
            Information about each term which appears in the annotation parents. Key is the ontolgy term. the value dict is:
            {
                    'total_annotations' : int
                        total number of annotations where this term appears (as a parent)
                    'total_sequences' : int
                        total number of sequences in annotations where this term appears (as a parent)
            }
            taxonomy : list of str
            The dbbact assigned taxonomy for each sequence (ordered in the same order as query sequences)
        }
    Details :
        Return a dict of details for all the annotations associated with at least one of the sequences used as input, and a list of seqpos and the associated annotationids describing it
        (i.e. a sparse representation of the annotations vector for the input sequence list)
    Validation:
        If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
        If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_fast_annotations', request)
    cfunc = get_fast_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    sequences = alldat.get('sequences')
    if sequences is None:
        return('sequences parameter missing', 400)
    region = alldat.get('region')
    get_term_info = alldat.get('get_term_info', True)
    get_taxonomy = alldat.get('get_taxonomy', True)
    get_parents = alldat.get('get_parents', True)
    use_sequence_translator = alldat.get('use_sequence_translator', True)
    dbname = alldat.get('dbname', None)
    if dbname is not None:
        use_sequence_translator = True
    get_all_exp_annotations = alldat.get('get_all_exp_annotations', True)
    if use_sequence_translator:
        seq_translate_api = g.seq_translate_api
    else:
        seq_translate_api = None
    err, annotations, seqannotations, term_info, taxonomy = dbannotations.GetFastAnnotations(g.con, g.cur, sequences, region=region, userid=current_user.user_id, get_term_info=get_term_info, get_taxonomy=get_taxonomy, get_parents=get_parents, get_all_exp_annotations=get_all_exp_annotations, seq_translate_api=seq_translate_api, dbname=dbname)
    if err:
        errmsg = 'error encountered while getting the fast annotations: %s' % err
        debug(6, errmsg)
        return(errmsg, 400)
    res = {'annotations': annotations, 'seqannotations': seqannotations, 'term_info': term_info, 'taxonomy': taxonomy}
    debug(2, 'returning fast annotations. res len is %d' % len(res))
    return json.dumps(res)


@login_required
@Seq_Flask_Obj.route('/sequences/get_taxonomy_annotation_ids', methods=['GET'])
@auto.doc()
def get_taxonomy_annotation_ids():
    """
    Title: Get taxonomy annotation ids
    Description : Get annotation ids for taxonomy substring
    URL: /sequences/get_taxonomy_annotation_ids
    Method: GET
    URL Params:
    Data Params: JSON
        {
            taxonomy : str
                the taxonomy substring to look for
        }
    Success Response:
        Code : 200
        Content :
        {
            annotationids : list of (int, int) (annotationid, count)
                the annotation ids and number of sequences from the taxonomy appearing in that annotation *for all annotations that contain at least 1 sequence from the requested taxonomy)
            seqids : list on int
                list of the sequenceids that have this taxonomy in the database
        }
    Details :
        Returns a list of annotationids. can get the annotation details for them via another api call to sequences/get_fast_annotations or sequences/
    Validation:
        If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
        If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_taxonomy_annotation_ids', request)
    cfunc = get_taxonomy_annotation_ids
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    taxonomy = alldat.get('taxonomy')
    if taxonomy is None:
        return('taxonomy parameter missing', 400)
    err, annotationids, seqids = dbsequences.GetTaxonomyAnnotationIDs(g.con, g.cur, taxonomy, userid=current_user.user_id)
    if err:
        errmsg = 'error encountered searching for taxonomy annotations for taxonomy %s: %s' % (taxonomy, err)
        debug(6, errmsg)
        return(errmsg, 400)
    return json.dumps({'annotationids': annotationids, 'seqids': seqids})


@login_required
@Seq_Flask_Obj.route('/sequences/get_taxonomy_annotations', methods=['GET'])
@auto.doc()
def get_taxonomy_annotations():
    """
    Title: Get taxonomy annotation ids
    Description : Get annotation ids for taxonomy substring
    URL: /sequences/get_taxonomy_annotation_ids
    Method: GET
    URL Params:
    Data Params: JSON
        {
            taxonomy : str
                the taxonomy substring to look for
        }
    Success Response:
        Code : 200
        Content :
        {
            'annotations' : list of (annotation, counts)
                the annotation details for all annotations that contain a sequence with the requested taxonomy (see /sequences/get_annotations) and the count of taxonomy sequences with the annotation
            seqids : list on int
                list of the sequenceids that have this taxonomy in the database
        }
    Validation:
        If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
        If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_taxonomy_annotations', request)
    cfunc = get_taxonomy_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    taxonomy = alldat.get('taxonomy')
    if taxonomy is None:
        return('taxonomy parameter missing', 400)
    err, annotations, seqids = dbsequences.GetTaxonomyAnnotations(g.con, g.cur, taxonomy, userid=current_user.user_id)
    if err:
        errmsg = 'error encountered searching for taxonomy annotations for taxonomy %s: %s' % (taxonomy, err)
        debug(6, errmsg)
        return(errmsg, 400)
    return json.dumps({'annotations': annotations, 'seqids': seqids})


@login_required
@Seq_Flask_Obj.route('/sequences/get_hash_annotations', methods=['GET'])
@auto.doc()
def get_hash_annotations():
    """
    Title: Get hash annotation ids
    Description : Get annotation ids for hash string
    URL: /sequences/get_hash_annotations
    Method: GET
    URL Params:
    Data Params: JSON
        {
            hash : str
                the hash substring to look for
        }
    Success Response:
        Code : 200
        Content :
        {
            'annotations' : list of (annotation, counts)
                the annotation details for all annotations that contain a sequence with the requested taxonomy (see /sequences/get_annotations) and the count of taxonomy sequences with the annotation
            seqids : list on int
                list of the sequenceids that have this taxonomy in the database
        }
    Validation:
        If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
        If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_hash_annotations', request)
    cfunc = get_hash_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    hash_str = alldat.get('hash')
    if hash_str is None:
        return('hash parameter missing', 400)
    err, annotations, seqids, seqnames = dbsequences.GetHashAnnotations(g.con, g.cur, hash_str, userid=current_user.user_id)
    if err:
        errmsg = 'error encountered searching for hash annotations for hash %s: %s' % (hash_str, err)
        debug(6, errmsg)
        return(errmsg, 400)
    return json.dumps({'annotations': annotations, 'seqids': seqids, 'seqstr': seqnames})

# superceded by 'dbname' in get_sequence_annotations() etc.
# @login_required
# @Seq_Flask_Obj.route('/sequences/get_gg_annotations', methods=['GET'])
# @auto.doc()
# def get_gg_annotations():
#     """
#     Title: Get annotation ids based on gg id
#     Description : Get annotation ids for gg id string
#     URL: /sequences/get_hash_annotations
#     Method: GET
#     URL Params:
#     Data Params: JSON
#         {
#             gg : str
#                 the gg id to look for
#         }
#     Success Response:
#         Code : 200
#         Content :
#         {
#             'annotations' : list of (annotation, counts)
#                 the annotation details for all annotations that contain a sequence with the requested taxonomy (see /sequences/get_annotations) and the count of taxonomy sequences with the annotation
#             seqids : list on int
#                 list of the sequenceids that have this taxonomy in the database
#         }
#     Validation:
#         If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
#         If annotation is not private, return it (no need for authentication)
#     """
#     debug(3, 'get_gg_annotations', request)
#     cfunc = get_gg_annotations
#     alldat = request.get_json()
#     if alldat is None:
#         return(getdoc(cfunc))
#     gg_str = alldat.get('gg_id')
#     if gg_str is None:
#         return('gg_id parameter missing', 400)
#     err, annotations, seqids, seqnames = dbsequences.GetGgAnnotations(g.con, g.cur, gg_str, userid=current_user.user_id)
#     if err:
#         errmsg = 'error encountered searching annotations for gg id %s: %s' % (gg_str, err)
#         debug(6, errmsg)
#         return(errmsg, 400)
#     return json.dumps({'annotations': annotations, 'seqids': seqids, 'seqstr': seqnames})


# @login_required
# @Seq_Flask_Obj.route('/sequences/get_silva_annotations', methods=['GET'])
# @auto.doc()
# def get_silva_annotations():
#     """
#     Title: Get annotation ids based on silva id
#     Description : Get annotation ids for silva id string
#     URL: /sequences/get_hash_annotations
#     Method: GET
#     URL Params:
#     Data Params: JSON
#         {
#             'silva_id' : str
#                 the silva id to look for (i.e. "LC133747.1.1482")
#         }
#     Success Response:
#         Code : 200
#         Content :
#         {
#             'annotations' : list of (annotation, counts)
#                 the annotation details for all annotations that contain a sequence with the requested taxonomy (see /sequences/get_annotations) and the count of taxonomy sequences with the annotation
#             'seqids' : list on int
#                 list of the sequenceids that have this taxonomy in the database
#             'seqstr': list of str
#                 list of the sequences that have this silva ids (i.e. ACGT of each seqid)
#         }
#     Validation:
#         If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
#         If annotation is not private, return it (no need for authentication)
#     """
#     debug(3, 'get_silva_annotations', request)
#     cfunc = get_silva_annotations
#     alldat = request.get_json()
#     if alldat is None:
#         return(getdoc(cfunc))
#     silva_str = alldat.get('silva_id')
#     if silva_str is None:
#         return('silva_id parameter missing', 400)
#     err, annotations, seqids, seqnames = dbsequences.GetSilvaAnnotations(g.con, g.cur, silva_str, userid=current_user.user_id)
#     if err:
#         errmsg = 'error encountered searching annotations for silva id %s: %s' % (silva_str, err)
#         debug(6, errmsg)
#         return(errmsg, 400)
#     return json.dumps({'annotations': annotations, 'seqids': seqids, 'seqstr': seqnames})


@Seq_Flask_Obj.route('/sequences/get_taxonomy_sequences', methods=['GET'])
@auto.doc()
def get_taxonomy_sequences():
    """
    Title: Get taxonomy sequences
    Description : Get a list of dbbact sequences with the given taxonomy substring
    URL: /sequences/get_taxonomy_sequences
    Method: GET
    URL Params:
    Data Params: JSON
        {
            taxonomy : str
                the taxonomy substring to look for
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
                    'seqid': int
                        the dbbact sequence id
                    'taxonomy': str
                        the taxonomy for the given sequence. semicolon separate format: k_XXX;f_YYY;...
                    'total_annotations': int
                        the number of annotations which this sequence is associated with
                    'total_experiments': int
                        the total number of experiments which this sequence is associated with
                }
        }
    Validation:
    """
    debug(3, 'get_taxonomy_sequences', request)
    cfunc = get_taxonomy_sequences
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    taxonomy = alldat.get('taxonomy')
    if taxonomy is None:
        return('taxonomy parameter missing', 400)
    seqids = dbsequences.get_taxonomy_seqids(g.con, g.cur, taxonomy, userid=None)
    err, sequences = dbsequences.SeqFromID(g.con, g.cur, seqids)
    if err:
        return err, err

    return json.dumps({'sequences': sequences})


@Seq_Flask_Obj.route('/sequences/get_info', methods=['GET'])
@auto.doc()
def get_sequence_info():
    """
    Title: Get sequences information
    Description : Get information (sequence, taxonomy) from sequence ids
    URL: /sequences/get_info
    Method: GET
    URL Params:
    Data Params: JSON
        {
            seqids : int or list of int
                the sequence ids to get information for
        }
    Success Response:
        Code : 200
        Content :
        {
    sequences : list of dict (one per sequence). contains:
        'seq' : str (ACGT)
            the sequence
        'taxonomy' : str
            the taxonomy of the sequence or '' if unknown
        'total_annotations': int
            the number of annotations which this sequence is associated with
        'total_experiments': int
            the total number of experiments which this sequence is associated with
        }
    Validation:
    """
    debug(3, 'get_sequence_info', request)
    cfunc = get_sequence_info
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    seqids = alldat.get('seqids')
    if seqids is None:
        return('seqids parameter missing', 400)
    err, sequences = dbsequences.SeqFromID(g.con, g.cur, seqids)
    if err:
        errmsg = 'error encountered searching for sequence information: %s' % err
        debug(6, errmsg)
        return(errmsg, 400)
    return json.dumps({'sequences': sequences})


@login_required
@Seq_Flask_Obj.route('/sequences/get_string_annotations', methods=['GET', 'POST', 'OPTIONS'])
# @crossdomain(origin='*', headers=['Content-Type'])
@auto.doc()
def get_sequence_string_annotations():
    """
    Title: Get sequence string annotations
    Description : Get description (string) and html link for all annotations of a given sequence
    URL: /sequences/get_string_annotations
    Method: GET, POST
    URL Params:
    Data Params: JSON
        {
            sequence : str
                the DNA sequence string to query the database (can be any length)
            region : int (optional)
                the region id (default=1 which is V4 515F 806R)
    Success Response:
        Code : 200
        Content :
        {
            "annotations" : list of
                {
                    "annotationid" : int
                        the id of the annotation
                    "annotation_summary" : str
                        String summarizing the annotation (i.e. 'higher in feces compared to saliva in homo spiens')
                }
        }
    Details :
        Validation:
            If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
            If annotation is not private, return it (no need for authentication)
    """
    debug(3, 'get_sequence_string_annotations', request)
    cfunc = get_sequence_string_annotations
    alldat = request.get_json()
    if alldat is None:
        return(getdoc(cfunc))
    sequence = alldat.get('sequence')
    if sequence is None:
        return('sequence parameter missing', 400)

    err, details = dbannotations.GetSequenceStringAnnotations(g.con, g.cur, sequence, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem geting details. error=%s' % err, 400)
    res = json.dumps({'annotations': details})
    return res

# Superceded by dbname='silva' in get_sequenceid() and similar functions
# @login_required
# @Seq_Flask_Obj.route('/sequences/get_seqs_from_external_db_id', methods=['GET', 'POST', 'OPTIONS'])
# @auto.doc()
# def api_get_seqs_from_db_id():
#     '''
#     Title: get_seqs_from_external_db_id
#     Description : Get all dbbact sequences that match the database_id supplied for silva/greengenes
#     URL: /sequences/get_seqs_from_external_db_id
#     Method: GET, POST
#     URL Params:
#     Data Params: JSON
#         {
#             seq_ids : list of str
#                 the sequence identifiers in the database (i.e. 'FJ978486.1.1387' for silva or '1111883' for greengenes)
#             database_name : str
#                 name of the database from which the ids originate. can be "silva" or "gg"
#     Success Response:
#         Code : 200
#         Content :
#         {
#             "dbbact_seqs_per_id": dict of {seq_id(str): tuple of (list of dbbact ids(int), list of dbbact sequences (str))}
#     '''
#     debug(3, 'api_seqs_from_external_db_id', request)
#     cfunc = api_get_seqs_from_db_id
#     alldat = request.get_json()
#     if alldat is None:
#         return(getdoc(cfunc))
#     seq_ids = alldat.get('seq_ids')
#     database_name = alldat.get('database_name')
#     if seq_ids is None:
#         return('seq_ids parameter missing', 400)
#     if database_name is None:
#         return('database_name parameter missing', 400)

#     dbbact_seqs = {}
#     for cid in seq_ids:
#         err, cdb_ids, cdb_seqs = dbsequences.get_seqs_from_db_id(g.con, g.cur, db_name=database_name, db_seq_id=cid)
#         if err:
#             debug(6, err)
#             return('Problem geting sequences for id %s. error=%s' % (cid, err), 400)
#         dbbact_seqs[cid] = (cdb_ids, cdb_seqs)
#     res = json.dumps({'dbbact_seqs_per_id': dbbact_seqs})
#     return res


# @login_required
# @Seq_Flask_Obj.route('/sequences/get_fast_annotations_external_db_id', methods=['GET'])
# @auto.doc()
# def get_fast_annotations_external_db_id():
#     """
#     Title: Get Fast Annotations from external database ids (i.e silva/gg)
#     Description : Get annotations for a list of external database sequences in a compressed form
#     URL: /sequences/get_fast_annotations
#     Method: GET
#     URL Params:
#     Data Params: JSON
#         {
#             seq_db_ids: list of str
#                 the silva/greengenes sequence identifiers to search for
#                 (i.e. 'FJ978486.1.1387' for silva or '1111883' for greengenes)
#             db_name : str
#                 the database for which the id originated. options are "silva" or "gg"
#             get_term_info : bool (optional)
#                 True (default) to get the information about each term, False to skip this step
#             get_all_exp_annotations: bool (optional)
#                 True (default) to get all annotations for each experiment which the sequence appear in at least one annotation.
#                 False to get just the annotations where the sequence appears

#     Success Response:
#         Code : 200
#         Content :
#         {
#             annotations: dict of (annotationid: details):
#                     annotationid : the annotationid used in seqannotations
#                     details:
#                 {
#                     "annotationid" : int
#                         the id of the annotation
#                     "user" : str
#                         name of the user who added this annotation
#                         (userName from UsersTable)
#                     "addedDate" : str (DD-MM-YYYY HH:MM:SS)
#                         date when the annotation was added
#                         (addedDate from CurationsTable)
#                     "expid" : int
#                         the ID of the experiment from which this annotation originated
#                         (uniqueId from ExperimentsTable)
#                         (see Query Experiment)
#                     "currType" : str
#                         curration type (differential expression/contaminant/etc.)
#                         (description from CurationTypesTable)
#                     "method" : str
#                         The method used to detect this behavior (i.e. observation/ranksum/clustering/etc")
#                         (description from MethodTypesTable)
#                     "agentType" : str
#                         Name of the program which submitted this annotation (i.e. heatsequer)
#                         (description from AgentTypesTable)
#                     "description" : str
#                         Free text describing this annotation (i.e. "lower in green tomatoes comapred to red ones")
#                     "private" : bool
#                         True if the curation is private, False if not
#                     "CurationList" : list of
#                         {
#                             "detail" : str
#                                 the type of detail (i.e. ALL/HIGH/LOW)
#                                 (description from CurationDetailsTypeTable)
#                             "term" : str
#                                 the ontology term for this detail (i.e. feces/ibd/homo sapiens)
#                                 (description from OntologyTable)
#                         }
#                     "parents" : list of tuples (type, list of terms)
#                         {
#                             type : type of the annotation type ('high'/'low','all')
#                             list of terms - list of ontology terms which are annotated or parents of annotated ontology term
#                         }
#                 }
#             seq_db_id_seqs : list of list of str
#                 the seqyences (ACGT) associated with each seq_db_id. in order of the query seq_db_ids
#                 (so the first entry is a list of all dbbact sequences associated with the first silva/gg id)

#             seq_db_id_annotations: list of (list of dict of {annotationid(int): count(int)})
#                 the ids and counts of the annotations matching each seq_db_id (i.e. silva/gg) ordered by the query order.
#                 the dbbact annotation ids (matching the annotations dict) are the key, and the number of dbbact sequences having this annotation is the value (note that each seq_db_id can appear in several dbbact sequences,
#                 and therefore may match several sequences with this annotation, so this will manifest in the count).
#             term_info : dict of {term, dict}:
#             Information about each term which appears in the annotation parents. Key is the ontolgy term. the value dict is:
#             {
#                     'total_annotations' : int
#                         total number of annotations where this term appears (as a parent)
#                     'total_sequences' : int
#                         total number of sequences in annotations where this term appears (as a parent)
#             }
#         }
#     Details :
#         Return a dict of details for all the annotations associated with at least one of the sequences used as input, and a list of seqpos and the associated annotationids describing it
#         (i.e. a sparse representation of the annotations vector for the input sequence list)
#     Validation:
#         If an annotation is private, return it only if user is authenticated and created the curation. If user not authenticated, do not return it in the list
#         If annotation is not private, return it (no need for authentication)
#     """
#     debug(3, 'get_fast_annotations_external_db_id', request)
#     cfunc = get_fast_annotations_external_db_id
#     alldat = request.get_json()
#     if alldat is None:
#         return(getdoc(cfunc))
#     seq_db_ids = alldat.get('seq_db_ids')
#     if seq_db_ids is None:
#         return('seq_db_ids parameter missing', 400)
#     db_name = alldat.get('db_name', 'silva')
#     get_term_info = alldat.get('get_term_info', True)
#     err, annotations, seq_db_id_seqs, term_info, seq_db_id_annotations = dbannotations.get_fast_annotations_gg_silva(g.con, g.cur, seq_db_ids, db_name=db_name, userid=current_user.user_id, get_term_info=get_term_info)
#     if err:
#         errmsg = 'error encountered while getting the external db fast annotations: %s' % err
#         debug(6, errmsg)
#         return(errmsg, 400)
#     res = {'annotations': annotations, 'seq_db_id_seqs': seq_db_id_seqs, 'term_info': term_info, 'seq_db_id_annotations': seq_db_id_annotations}
#     return json.dumps(res)


@login_required
@Seq_Flask_Obj.route('/sequences/get_primers', methods=['GET', 'POST', 'OPTIONS'])
@auto.doc()
def get_primers():
    '''
    Title: get_primers
    Description : Get information about all the sequencing primers used in dbbact
    URL: /sequences/get_primers
    Method: GET, POST
    URL Params:
    Data Params: JSON
        {
        }
    Success Response:
        Code : 200
        Content :
        {
            "primers": list of dict of {
                'primerid': int
                    dbbact internal id of the primer region (i.e. 1 for v4, etc.)
                'name': str,
                    name of the primer region (i.e. 'v4', 'its1', etc.)
                'fprimer': str
                'rprimer: str
                    name of the forward and reverse primers for the region (i.e. 515f, etc.)
                'fprimerseq': str
                    the concensus sequence for the forward primer
            }
        }
    '''
    debug(3, 'get_primers', request)
    err, primers = dbprimers.get_primers(g.con, g.cur)
    if err:
        debug(6, err)
        return ('Problem geting primers. error=%s' % err, 400)
    res = json.dumps({'primers': primers})
    return res


@login_required
@Seq_Flask_Obj.route('/sequences/add_primer_region', methods=['POST'])
@auto.doc()
def add_primer_region():
    '''
    Title: add_primer_region
    Description : Add a new primer region (i.e. 'V4') to the primers table
    URL: /sequences/add_primer_region
    Method: POST
    URL Params:
    Data Params: JSON
        {
        'name': str
            name of the region (i.e. 'v4')
        'fprimer', 'rprimer': str, optional
            name (i.e. '515f') or sequence pf the forward and reverse primers used for the region
        }
    Success Response:
        Code : 200
        Content :
        {
            "primers": list of dict of {
                'primerid': int
                    dbbact internal id of the primer region (i.e. 1 for v4, etc.)
                'name': str,
                    name of the primer region (i.e. 'v4', 'its1', etc.)
                'fprimer': str
                'rprimer: str
                    name of the forward and reverse primers for the region (i.e. 515f, etc.)
            }
        }
    '''
    debug(3, 'get_primers', request)
    try:
        alldat = request.get_json()
        regionname = alldat.get('name')
        forwardprimer = alldat.get('fprimer', '')
        reverseprimer = alldat.get('rprimer', '')
    except Exception as e:
        msg = 'missing parameters for add_primer_region'
        debug(2, msg)
        return(json.dumps(msg), 400)

    err = dbprimers.AddPrimerRegion(g.con, g.cur, regionname=regionname, forwardprimer=forwardprimer, reverseprimer=reverseprimer, userid=current_user.user_id)
    if err:
        debug(6, err)
        return ('Problem adding new region. error=%s' % err, 400)
    res = json.dumps('ok')
    return res


@Seq_Flask_Obj.route('/sequences/guess_region', methods=['GET'])
@auto.doc()
def guess_region():
    '''
    Title: guess_region
    Description : Suggest the primer region for the list of sequences
    URL: /sequences/guess_region
    Method: GET
    URL Params:
    Data Params: JSON
        {
            "sequence": list of str
                the sequences (ACGT) to get the primer region for
        }
    Success Response:
        Code : 200
        Content :
        {
            "region": str
                name of the primer region containing all the sequences matching dbbact
            "regionid": int
                id of the primer region
        }
    '''
    debug(3, 'guess_region', request)
    cfunc = add_sequences
    alldat = request.get_json()
    sequences = alldat.get('sequences')
    if sequences is None:
        return(getdoc(cfunc))
    err, primerid, primer_name = dbsequences.get_sequences_primer(g.con, g.cur, sequences)
    if err:
        return ('Problem guessing sequences region. error=%s' % err, 400)
    return json.dumps({'region': primer_name, 'regionid': primerid})
