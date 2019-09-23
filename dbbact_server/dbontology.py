import psycopg2
from .utils import debug, tolist
from . import dbidval
from . import dbannotations


def AddTerm(con, cur, term, parent='na', ontologyname='scdb', synonyms=[], commit=True):
    """
    Add a term to the ontology table. Also add parent and synonyms if supplied

    input:

    output:

    """
    try:
        # convert everything to lower case before interacting with the database
        term = term.lower()
        parent = parent.lower()
        ontologyname = ontologyname.lower()
        if synonyms is None:
            synonyms = []
        synonyms = [csyn.lower() for csyn in synonyms]

        # add/get the ontology term
        err, termid = dbidval.AddItem(con, cur, table='OntologyTable', description=term, commit=False)
        if err:
            return err, None
        # add/get the ontology parent term
        err, parentid = dbidval.AddItem(con, cur, table='OntologyTable', description=parent, commit=False)
        if err:
            return err, None
        # add/get the ontology name
        err, ontologynameid = dbidval.AddItem(con, cur, table='OntologyNamesTable', description=ontologyname, commit=False)
        if err:
            return err, None
        # add the tree info
        err, treeid = AddTreeTerm(con, cur, termid, parentid, ontologynameid, commit=False)
        if err:
            return err, None
        # add the synonyms
        if synonyms:
            for csyn in synonyms:
                err, cid = AddSynonym(con, cur, termid, csyn, commit=False)
        debug(2, 'added ontology term %s. id is %d' % (term, termid))
        if commit:
            con.commit()
        return '', termid

    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in ontology.AddTerm" % e)
        return "error %s enountered in ontology.AddTerm" % e, -2


def AddTreeTerm(con, cur, termid, parentid, ontologynameid, commit=True):
    """
    Add a relation to the OntologyTreeTable

    input:
    con,cur
    termid : int
        the ontology term id (from OntologyTable)
    parentid : int
        the parent ontology term id (from OntologyTable)
    ontologynameid : int
        the id of the name of the ontology (from OntologyNamesTable)
    commit : bool (optional)
        True (default) to commit, False to not commit to database

    output:
    err : str
        Error message or empty string if ok
    sid : int
        the id of the added item
    """
    try:
        # test if already exists
        cur.execute('SELECT uniqueId FROM OntologyTreeStructureTable WHERE (ontologyId=%s AND ontologyParentId=%s AND ontologyNameId=%s) LIMIT 1', [termid, parentid, ontologynameid])
        if cur.rowcount > 0:
            sid = cur.fetchone()[0]
            debug(2, 'Tree entry exists (%d). returning it' % sid)
            return '', sid
        # does not exist - lets add it
        cur.execute('INSERT INTO OntologyTreeStructureTable (ontologyId,ontologyParentId,ontologyNameId) VALUES (%s,%s,%s) RETURNING uniqueId', [termid, parentid, ontologynameid])
        sid = cur.fetchone()[0]
        return '', sid
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in ontology.AddTreeTerm" % e)
        return "error %s enountered in ontology.AddTreeTerm" % e, -2


def AddSynonym(con, cur, termid, synonym, commit=True):
    """
    Add a synonym to OntologySynonymTable

    input:
    con,cur
    termid : int
        The idOntology value to add (from OntologyTable)
    synonym : str
        The synonymous term
    commit : bool (optional)
        True (default) to commit, False to skip commit

    output:
    err : str
        Error message or empty string if ok
    sid : int
        the id of the added synonym
    """
    try:
        synonym = synonym.lower()
        # TODO: maybe test idterm,synonym does not exist
        cur.execute('INSERT INTO OntologySynonymTable (idOntology,synonym) VALUES (%s,%s) RETURNING uniqueId', [termid, synonym])
        sid = cur.fetchone()[0]
        if commit:
            con.commit()
        return '', sid
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in ontology.AddSynonym" % e)
        return "error %s enountered in ontology.AddSynonym" % e, -2


def GetTreeParentsById(con, cur, termid):
    """
    get the parent (name and id) by term id

    input:
    con,cur
    termid : int
        the term to get the parent for

    output:
    err : str
        Error message or empty string if ok
    parentids : list of int
        list of ids of all the immediate parents of the term
    """
    try:
        cur.execute('SELECT ontologyParentId FROM OntologyTreeStructureTable WHERE ontologyId=%s', [termid])
        if cur.rowcount == 0:
            debug(3, 'termid %d not found in ontologytree' % termid)
            return 'termid %d not found in ontologytree' % termid, []
        parentids = []
        for cres in cur:
            parentids.append(cres[0])
        debug(2, 'found %d parentids for termid %d' % (len(parentids), termid))
        return '', parentids
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in ontology.GetTreeParentById" % e)
        return "error %s enountered in ontology.GetTreeParentById" % e, '', []


def GetParents(con, cur, term):
    """
    Get all the parents of the term in the ontology tree

    input:
    con,cur
    term : str
        The term for which to look for parents

    output:
    err : str
        Error message or empty string if ok
    parents : list of str
        the parents of term
    """
    termid = dbidval.GetIdFromDescription(con, cur, 'OntologyTable', term)
    if termid < 0:
        err, termid = GetSynonymTermId(con, cur, term)
        if err:
            debug(3, 'ontology term not found for %s' % term)
            return 'ontolgy term %s not found' % term, []
        debug(2, 'converted synonym to termid')
    plist = [termid]
    parents = [term]
    parents_id_set = set()
    while len(plist) > 0:
        cid = plist.pop(0)
        origid = cid
        if cid in parents_id_set:
            continue
        err, cparentids = GetTreeParentsById(con, cur, cid)
        if err:
            continue
        plist.extend(cparentids)
        for cid in cparentids:
            err, cparent = dbidval.GetDescriptionFromId(con, cur, 'OntologyTable', cid)
            if err:
                continue
            parents.append(cparent)
        parents_id_set.add(origid)
    debug(2, 'found %d parents' % len(parents))
    return '', parents


def GetSynonymTermId(con, cur, synonym):
    """
    Get the term id for which the synonym is

    input:
    con,cur
    synonym : str
        the synonym to search for

    output:
    err : str
        Error message or empty string if ok
    termid : int
        the id of the term for the synonym is defined
    """
    synonym = synonym.lower()
    try:
        cur.execute('SELECT idOntology FROM OntologySynonymTable WHERE synonym=%s', [synonym])
        if cur.rowcount == 0:
            debug(2, 'synonym %s not found' % synonym)
            return 'synonym %s not found' % synonym, -1
        termid = cur.fetchone()[0]
        debug(2, 'for synonym %s termid is %d' % (synonym, termid))
        return '', termid
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in GetSynonymTermId" % e)
        return "error %s enountered in GetSynonymTermId" % e, -2


def GetSynonymTerm(con, cur, synonym):
    """
    Get the term for which the synonym is

    input:
    con,cur
    synonym : str
        the synonym to search for

    output:
    err : str
        Error message or empty string if ok
    term : str
        the term for the synonym is defined
    """
    err, termid = GetSynonymTermId(con, cur, synonym)
    if err:
        debug(2, 'ontology term %s is not a synonym' % synonym)
        return err, str(termid)
    err, term = dbidval.GetDescriptionFromId(con, cur, 'ontologyTable', termid)
    if err:
        debug(3, 'ontology term not found for termid %d (synonym %s)' % (termid, synonym))
        return err, term
    return '', term


def GetTermAnnotations(con, cur, terms, use_synonyms=True, get_children=True):
    '''
    Get details for all annotations which contain the ontology term "term" as a parent of (or exact) annotation detail

    Parameters
    ----------
    con, cur
    terms : str or list of str
        the ontology term to search. if list, retrieve only annotations containing all the terms in the list
    use_synonyms : bool (optional)
        True (default) to look in synonyms table if term is not found. False to look only for exact term
    get_children: bool, optional
        True to get annotations of all term children (i.e. get also annotations with feces when you search for excreta)

    Returns
    -------
    err: str
        empty str ('') if ok, otherwise error returned
    annotations : list of dict
        list of annotation details per annotation which contains the term
    '''
    debug(1, 'GetTermAnnotations for ontology terms %s, use_synonyms=%s, get_children=%s' % (terms, use_synonyms, get_children))
    terms = tolist(terms)
    annotation_ids = None
    if len(terms) == 0:
        return 'No terms in query', []
    for cterm in terms:
        cterm = cterm.lower()
        if get_children:
            cur.execute('SELECT idannotation FROM AnnotationParentsTable WHERE ontology=%s', [cterm])
            if cur.rowcount == 0:
                if use_synonyms:
                    err, cterm = GetSynonymTerm(con, cur, cterm)
                    if err:
                        debug(3, 'no annotations or synonyms for term %s' % cterm)
                        return '', []
                    debug(1, 'found original ontology term %s' % cterm)
                    cur.execute('SELECT idannotation FROM AnnotationParentsTable WHERE ontology=%s', [cterm])
                else:
                    debug(3, 'no annotations for term %s' % cterm)
                    return '', []
        else:
            ctermid = dbidval.GetIdFromDescription(con, cur, 'OntologyTable', cterm)
            if ctermid < 0:
                if use_synonyms:
                    err, ctermid = GetSynonymTermId(con, cur, cterm)
                if err:
                    msg = 'ontology term not found for %s' % cterm
                    debug(3, msg)
                    return msg, []
                debug(2, 'converted synonym to termid')
            cur.execute('SELECT idannotation FROM AnnotationListTable WHERE idontology=%s', [ctermid])
        res = cur.fetchall()
        cannotation_ids = set()
        for cres in res:
            cannotation_ids.add(cres[0])
        if annotation_ids is None:
            annotation_ids = cannotation_ids
        annotation_ids = annotation_ids.intersection(cannotation_ids)

    annotations = []
    for cannotation_id in annotation_ids:
        err, cdetails = dbannotations.GetAnnotationsFromID(con, cur, cannotation_id)
        if err:
            debug(6, err)
            continue
        annotations.append(cdetails)
    debug(3, 'found %d annotations' % len(annotations))
    return '', annotations


def get_term_counts(con, cur, terms, term_types=('single'), ignore_lower=False):
    '''Get the number of annotations and experiments containing each term in terms.
    NOTE: terms can be also term pairs (term1+term2)

    Parameters
    ----------
    terms: list of str
        list of terms to look for. can be term pairs
    TODO: ignore_lower: bool, optional. TODO
        True to look for total counts combining "all"/"high" and "lower" counts

    Returns
    -------
    dict of {term(str): {'total_annotations': int, 'total_experiments': int}}
    '''
    debug(1, 'get_term_counts for %d terms' % len(terms))
    terms = list(set(terms))
    term_info = {}
    for cterm in terms:
        cur.execute('SELECT TotalExperiments, TotalAnnotations from TermInfoTable WHERE term=%s LIMIT 1', [cterm])
        # cur.execute('SELECT seqCount, annotationCount, exp_count from OntologyTable WHERE description=%s LIMIT 1', [cterm])
        if cur.rowcount == 0:
            debug(2, 'Term %s not found in ontology table' % cterm)
            continue
        res = cur.fetchone()
        term_info[cterm] = {}
        # term_info[cterm]['total_sequences'] = 0
        term_info[cterm]['total_experiments'] = res[0]
        term_info[cterm]['total_annotations'] = res[1]
    debug(1, 'found info for %d terms' % len(term_info))
    return term_info


def get_annotations_term_counts(con, cur, annotations):
    '''
    Get information about all ontology terms in annotations

    Parameters
    ----------
    con, cur
    annotations : list of annotations
        The list of annotations to get the terms for (see dbannotations.GetAnnotationsFromID() )

    Returns
    -------
    term_info : dict of {str: dict}:
        Key is the ontology term.
        Value is a dict of pairs:
            'total_annotations' : int
                The total number of annotations where this ontology term is a predecessor
            'total_squences' : int
                The total number of sequences in annotations where this ontology term is a predecessor
    '''
    debug(1, 'get_annotations_term_counts for %d annotations' % len(annotations))
    terms = []
    for cannotation in annotations:
        for ctype, cterm in cannotation['details']:
            if ctype == 'low':
                cterm = '-' + cterm
            terms.append(cterm)
    terms = list(set(terms))
    # return GetTermCounts(con, cur, terms)
    return get_term_counts(con, cur, terms)


def get_ontology_terms_list(con, cur, min_term_id=None, ontologyid=None):
    '''
    Get list of all ontology terms

    Parameters
    ----------
    con, cur
    min_term_id: int or None, optional
        if int, get only terms with dbbactid > min_term_id (for fast syncing)
        if None, get all terms
    ontologies: list of str, optional
        if not None, get only terms from ontologies in ontologies list
        if None, get terms from all ontologies
        TODO: NOT SUPPORTED YET!

    Returns
    -------
    terms : dict of {term(str): id(int)}
        The list of ontology terms from table OntologyTable
    '''
    # get rid of duplicate terms
    debug(1, 'GetListOfOntologies')
    if ontologyid is None:
        cur.execute('SELECT id, description from ontologyTable')
    else:
        cur.execute('SELECT ontologytreestructuretable.ontologyid, ontologytable.description FROM ontologytreestructuretable INNER JOIN ontologytable ON ontologytable.id=ontologytreestructuretable.ontologyid WHERE OntologyNameID=%s', [ontologyid])

    if cur.rowcount == 0:
        debug(1, 'Ontologies list is empty')
        return

    if min_term_id is None:
        min_term_id = 0

    res = cur.fetchall()
    all_ontologies = {}
    for cres in res:
        if cres[0] > min_term_id:
            all_ontologies[cres[1]] = cres[0]
    return all_ontologies


def GetListOfSynonym(con, cur):
    '''
    Get list of synonym

    Parameters
    ----------
    con, cur

    Returns
    -------
    terms : list of str
        The full list of synonym
    '''
    # get rid of duplicate terms
    debug(1, 'GetListOfSynonym')
    all_synonym = []
    cur.execute('SELECT distinct synonym from ontologysynonymtable')
    if cur.rowcount == 0:
        debug(1, 'ontologysynonymtable list is empty')
        return

    res = cur.fetchall()
    all_synonym = []
    for cres in res:
        all_synonym.append(cres[0])
    return all_synonym


def GetIDs(con, cur, ontList):
    """
    Get ids of list of ontologies

    Parameters
    ----------
    con,cur : database connection and cursor
    ontList: list of str
        the terms to get the ids for

    Returns
    -------
    errmsg : str
        "" if ok, error msg if error encountered
    termids : list of int or None
        list of the new ids or None if error enountered
    """
    ontids = []
    try:
        sqlStr = "SELECT id from ontologyTable WHERE (description='%s')" % ontList[0]
        idx = 0
        while idx < len(ontList):
            sqlStr += " OR (description='%s')" % ontList[idx]
            idx = idx + 1

        cur.execute(sqlStr)
        if cur.rowcount == 0:
            debug(2, 'Failed to get list of terms')
        else:
            res = cur.fetchall()
            for cres in res:
                ontids.append(res[0])

        debug(3, "Number of ontology ids %d (out of %d)" % (len(ontids), len(ontList)))
        return "", ontids

    except psycopg2.DatabaseError as e:
        debug(7, 'database error %s' % e)
        return "database error %s" % e, None


def get_terms_from_ids(con, cur, ids):
    '''Get names of terms from ids list

    Parameters
    ----------
    con,cur : database connection and cursor
    ids: list of int
        the ids to get the names for

    Returns
    -------
    dict of {id(int): name(str)}
    '''
    names = {}
    for cid in ids:
        cur.execute('SELECT description FROM OntologyTable WHERE id=%s LIMIT 1', [cid])
        if cur.rowcount > 0:
            names[cid] = cur.fetchone()[0]
        else:
            names[cid] = 'NOT FOUND'
    return names


def get_term_pairs_count(con, cur, term_pairs):
    '''Get the total count of experiments where each term pair appears

    Parameters
    ----------
    con,cur : database connection and cursor
    term_pairs: list of str
        the term pairs to count

    Returns
    -------
    term_count: dict of {term(str): count(float)}
    '''
    term_count = {}
    for cterm in term_pairs:
        cur.execute("SELECT AnnotationCount from TermPairsTable WHERE TermPair=%s", [cterm])
        if cur.rowcount == 0:
            debug(5, 'term pair %s not found' % cterm)
            term_count[cterm] = 0
            continue
        res = cur.fetchone()
        term_count[cterm] = res[0]
    debug(2, 'Found term pairs for %d terms' % len(term_count))
    return term_count


def get_ontology_id_from_name(con, cur, ontology_name):
    '''Get the id of an ontology (i.e. "doid") based on it's name

    Parameters
    ----------
    con,cur : database connection and cursor
    ontology_name: str
        name of the ontology (i.e. "doid"/"envo" etc.)

    Returns
    -------
    err: empty ('') if ok, otherwise the error enoucntered
    oid: int - the ontology id
    '''
    ontology_name = ontology_name.lower()
    cur.execute('SELECT id FROM OntologyNamesTable WHERE description=%s LIMIT 1', [ontology_name])
    if cur.rowcount > 0:
        oid = cur.fetchone()[0]
    else:
        msg = 'ontology name %s not found'
        debug(8, msg)
        return msg, -1
    debug(1, 'found ontology id %d for ontology %s' % (oid, ontology_name))
    return '', oid


def get_term_children(con, cur, term, ontology_name=None, only_annotated=True):
    '''get a list of all terms that are a children of the given term. Optionally, limit to a given ontology.

    Parameters
    ----------
    con,cur : database connection and cursor
    term: str
        The term to get the children for
    ontology_name: str or None, optional
        if not None, limit to children based on a give ontology name (matching OntologyNamesTable. i.e. "envo"/"uberon"/"doid"/"efo"/"gaz"/"scdb" etc.)
    only_annotated: bool, optional
        if True, return only child terms that have at least one annotation in their subtree

    Returns
    -------
    dict of {termid(int): term(str)}
    All the children of the given term
    '''
    err, termid = GetIDs(con, cur, [term])
    if err:
        return err, {}
    if len(termid) == 0:
        msg = 'term %s not found' % term
        debug(4, msg)
        return msg, {}
    termid = termid[0]
    children_ids = set()
    terms = set(termid)
    if ontology_name is None:
        ontology_id = None
    else:
        err, ontology_id = get_ontology_id_from_name(con, cur, ontology_name)
        if err:
            return err, {}
    while len(terms) > 0:
        ctermid = terms.pop()
        if ctermid in children_ids:
            debug(8, 'termid %d is in a circle?' % ctermid)
            continue
        children_ids.add(ctermid)
        if ontology_name is None:
            cur.execute('SELECT ontologyid FROM OntologyTreeStructureTable WHERE ontologyParentID=%s', [ctermid])
        else:
            cur.execute('SELECT ontologyid FROM OntologyTreeStructureTable WHERE ontologyParentID=%s AND ontologyNameID=%s', [ctermid, ontology_id])
        res = cur.fetchall()
        for cres in res:
            terms.add(cres[0])
    debug(5, 'found %d children for term %s' % (len(children_ids), term))
    children = get_terms_from_ids(con, cur, children_ids)
    if only_annotated:
        ok_terms = set()
        for cterm in children.values():
            # look if term has any annotations
            cur.execute('SELECT TotalAnnotations from TermInfoTable WHERE term=%s LIMIT 1', [cterm])
            if cur.rowcount > 0:
                if cur.fetchone()[0] > 0:
                    ok_terms.add(cterm)
                break
            # look also for lower in annotations
            cur.execute('SELECT TotalAnnotations from TermInfoTable WHERE term=%s LIMIT 1', ['-' + cterm])
            if cur.rowcount > 0:
                if cur.fetchone()[0] > 0:
                    ok_terms.add(cterm)
        debug(3, 'found %d term children with annotations out of %d children' % (len(ok_terms), len(children)))
        new_children = {}
        for cid, cterm in children.items():
            if cterm in ok_terms:
                new_children[cid] = cterm
        children = new_children
    return '', children
