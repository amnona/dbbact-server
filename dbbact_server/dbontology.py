import psycopg2
from .utils import debug, tolist
from . import dbidval
from . import dbannotations


def add_ontology_term(con, cur, term, term_id='', commit=True):
    '''add a term to the OntologyTable (if does not exist) and return it's id
    NOTE: if the term in the database only appears without a term_id, and a term_id is supplied, update the term_id

    Parameters
    ----------
    con, cur
    term: str
        the term to add (i.e. 'feces')
    term_id: str, optional
        the ontology id for the term (unique identifier, i.e. 'UBERON:0001988')
        if empty (i.e. ''), add new term_id in the form of 'dbbact:XXXX' where XXX is the new term id in dbbact
    commit: bool, optional
        True to commit the changes to the database

    Returns
    -------
    err: str
        empty '' if ok, otherwise error encountered
    termid: int
        the dbbact term id
    '''
    try:
        # no term_id, find or create a new entry with term_id=''
        if term_id == '':
            err, termid = dbidval.AddItem(con, cur, table='OntologyTable', description=term, commit=False)
            if err:
                return err, None
            term_id = 'dbbact:%d' % termid
            cur.execute('UPDATE OntologyTable SET term_id=%s WHERE id=%s', [term_id, termid])
        else:
            # term_id supplied
            cur.execute('SELECT id FROM OntologyTable WHERE description=%s AND term_id=%s', [term, term_id])
            # if already in the table, return it
            if cur.rowcount > 0:
                termid = cur.fetchone()[0]
            else:
                cur.execute('SELECT id FROM OntologyTable WHERE description=%s AND term_id=%s', [term, ''])
                # if already in the table but with no term_id, just update the term_id
                if cur.rowcount > 0:
                    termid = cur.fetchone()[0]
                    cur.execute('UPDATE OntologyTable SET term_id=%s WHERE id=%s', [term_id, termid])
                else:
                    # not in the table - create a new entry
                    cur.execute('INSERT INTO OntologyTable (description, term_id) VALUES (%s, %s) RETURNING id', [term, term_id])
                    termid = cur.fetchone()[0]
        return '', termid
    except psycopg2.DatabaseError as e:
        msg = "error %s in add_ontology_term" % e
        debug(8, msg)
        return msg, -2


def get_term_ids(con, cur, term, allow_ontology_id=True):
    '''Get a list of dbbact term ids matching the term
    NOTE: can handle terms (i.e. 'feces') or ontology ids (i.e. 'envo:000001')
    NOTE: can be more than one result since same term can appear in several ontologies

    Parameters
    ----------
    con, cur
    term: str
        the term to get the ids for
    allow_ontology_id: bool, optional
        if True, term can also be an ontology id (i.e. 'envo:000001')

    Returns
    -------
    error (str):
        empty string ('') if ok, otherwise the error encountered
    ids (list of int)
        the dbbact term ids matching the term. NOTE: if term not found, will not error and instead return empty list.
    '''
    term = term.lower()
    term_found = False

    # try first the term_id field (i.e. gaz:0004)
    if allow_ontology_id:
            cur.execute('SELECT id FROM OntologyTable WHERE term_id=%s', [term])
            if cur.rowcount > 0:
                term_found = True
    # if not found, try next the term description field (i.e. feces/homo sapiens)
    if not term_found:
        cur.execute('SELECT id FROM OntologyTable WHERE description=%s', [term])
        if cur.rowcount > 0:
            term_found = True

    if not term_found:
        msg = 'Term %s not found in OntologyTable' % term
        debug(2, msg)
        return '', []

    ids = []
    res = cur.fetchall()
    for cres in res:
        ids.append(cres['id'])
    debug(2, 'found %d matches for term %s' % (len(ids), term))
    return '', ids


def get_name_from_id(con, cur, term_dbbact_id):
    '''Get term name and term_id from the term dbbact id (OntologyTable)

    Parameters
    ----------
    con, cur
    term_dbbact_id: int
        the dbbact id of the term to get the name and term_id for

    Returns
    -------
    err: str.
        Empty ('') if ok otherwise the error encountered
    term: str
        The matching term description (i.e. 'feces' etc.)
    term_id: str
        the matching term_id (i.e. 'envo:00001' etc.)
    '''
    try:
        cur.execute('SELECT description, term_id FROM OntologyTable WHERE id=%s LIMIT 1', [term_dbbact_id])
        if cur.rowcount == 0:
            msg = 'termid %d not in OntologyTable' % term_dbbact_id
            debug(5, msg)
            return msg, None, None
        res = cur.fetchone()
        return '', res['description'], res['term_id']
    except psycopg2.DatabaseError as e:
        msg = "error %s enountered in ontology.get_name_from_id" % e
        debug(7, msg)
        return msg, None, None


def get_names_from_ids(con, cur, term_ids):
    '''Get term names from the dbbact ids (OntologyTable)

    Parameters
    ----------
    con, cur
    term_ids: list of int
        the dbbact ids of the terms to get the names for

    Returns
    -------
    err: str.
        Empty ('') if ok otherwise the error encountered
    terms: list of str.
        The matching term descriptions (i.e. 'feces' etc.)
    ontology_ids: list of str
        the matching ontology ids (i.e. 'envo:00001' etc.)
    '''
    terms = []
    ontology_ids = []
    try:
        for cid in term_ids:
            err, cterm, cterm_id = get_name_from_id(con, cur, cid)
            if err:
                debug(5, err)
                return err, [], []
            terms.append(cterm)
            ontology_ids.append(cterm_id)
        return '', terms, ontology_ids
    except psycopg2.DatabaseError as e:
        msg = "error %s enountered in ontology.get_names_from_ids" % e
        debug(7, msg)
        return msg, [], []


def AddTerm(con, cur, term, parent='dbbact:1811274', ontologyname='dbbact', synonyms=[], term_id='', parent_id='', commit=True):
    """
    Add a term to the ontology table. Also add parent and synonyms if supplied

    Parameters
    ----------
    con, cur
    term: str
        the term description to add (i.e. 'feces')
    parent: str, optional
        the name of the parent term (i.e. 'excreta')
        if 'dbbact:1811274', means no parent for this term (for example when new term not from existing ontology) (it is 'dbbact root')
    ontologyname: str, optional
        name of the ontology to which this term/parent link belongs (i.e. 'envo')
    synonyms: list of str, optional
        synonyms for this term (i.e. 'poop')
    term_id: str, optional
        the ontology id for the term (unique identifier, i.e. 'UBERON:0001988')
    parent_id: str, optional
        the term_id for the parent. if not empty, will be used in AND with the parent field for exact identification
    commit: bool, optional
        True to commit the changes to the database

    Returns
    -------
    err: str
        empty ('') if ok, otherwise the error encountered
    termid: int
        the term id for the new term
    """
    try:
        # check if we're trying to add a term from an ontology (i.e. the term is the term_id)
        ts = term.split(':')
        if len(ts) > 1:
            msg = 'AddTerm failed. Cannot add a term that contains ":". Maybe it is the wrong ID?'
            debug(6, msg)
            return msg, -1
        # convert everything to lower case before interacting with the database
        term = term.lower()
        parent = parent.lower()
        term_id = term_id.lower()
        if parent_id is None:
            debug(4, 'parent id for term %s is None. Changed to empty' % term)
            parent_id = ''
        parent_id = parent_id.lower()
        ontologyname = ontologyname.lower()
        if synonyms is None:
            synonyms = []
        synonyms = [csyn.lower() for csyn in synonyms]

        # add/get the ontology term
        err, termid = add_ontology_term(con, cur, term, term_id, commit=False)
        if err:
            return err, None

        # add/get the ontology parent term
        err, parentid = add_ontology_term(con, cur, parent, parent_id, commit=False)
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
    get the parent ids for a given term id

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


def GetTreeChildrenById(con, cur, termid):
    """
    get the children ids for a given term id

    input:
    con,cur
    termid : int
        the term to get the children for

    output:
    err : str
        Error message or empty string if ok
    childids : list of int
        list of ids of all the immediate children of the term
    """
    try:
        cur.execute('SELECT ontologyId FROM OntologyTreeStructureTable WHERE ontologyParentId=%s', [termid])
        if cur.rowcount == 0:
            debug(3, 'termid %d not found in ontologytree' % termid)
            return 'termid %d not found in ontologytree' % termid, []
        childids = []
        for cres in cur:
            childids.append(cres[0])
        debug(2, 'found %d child ids for termid %d' % (len(childids), termid))
        return '', childids
    except psycopg2.DatabaseError as e:
        debug(7, "error %s enountered in ontology.GetTreeChildrenById" % e)
        return "error %s enountered in ontology.GetTreeChildrenById" % e, '', []


def GetParents_old(con, cur, term):
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
    # termid = dbidval.GetIdFromDescription(con, cur, 'OntologyTable', term)
    err, termids = get_term_ids(con, cur, term)
    if err:
        debug(3, err)
        return err, []
    # if termid < 0:
    if len(termids) == 0:
        err, termid = GetSynonymTermId(con, cur, term)
        if err:
            debug(3, 'ontology term not found for %s' % term)
            return 'ontolgy term %s not found' % term, []
        debug(2, 'converted synonym to termid')
        termids = [termid]
    # plist = [termid]
    plist = termids
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


def get_parents_ids(con, cur, term_ids):
    """
    Get dbbact ids of all the parents of a given term_id in the ontology tree

    input:
    con,cur
    term_ids : list of int
        The dbbact ids (in OntologyTable) for the term for which to look for parents

    output:
    err : str
        Error message or empty string if ok
    parents : list of int
        the ids of the parents of term (in OntologyTable)
    """
    # termid = dbidval.GetIdFromDescription(con, cur, 'OntologyTable', term)

    plist = term_ids.copy()
    parents_ids = set(plist)
    processed_set = set()
    while len(plist) > 0:
        cid = plist.pop(0)
        if cid in processed_set:
            continue
        err, cparentids = GetTreeParentsById(con, cur, cid)
        if err:
            continue
        plist.extend(cparentids)
        parents_ids = parents_ids.union(cparentids)
        processed_set.add(cid)
    debug(2, 'found %d parents' % len(parents_ids))
    return '', parents_ids


def get_parents(con, cur, term, force_unique=False):
    """
    Get all the parents of the term in the ontology tree

    input:
    con,cur
    term : str
        The term ('feces' or 'efo:00001' etc.) for which to look for parents
    force_unique: bool, optional
        True to fail if more than one id matches the term
        False (default) to return parents for all the terms

    output:
    err : str
        Error message or empty string if ok
    parent_dbbact_ids : list of int
        the dbbact term ids of the given term
    """
    err, term_dbbact_ids = get_term_ids(con, cur, term, allow_ontology_id=True)
    if err:
        return err, []
    if len(term_dbbact_ids) > 1:
        if force_unique:
            msg = 'more than one id (%d) found for term %s. Please supply ontology id instead (i.e. "envo:00001")' % (len(term_dbbact_ids), term)
            debug(4, msg)
            return msg, []
    err, parent_dbbact_ids = get_parents_ids(con, cur, term_dbbact_ids)
    if err:
        return err, []
    return '', list(parent_dbbact_ids)


def get_parents_terms_and_term_ids(con, cur, term, force_unique=False):
    """
    Get all the parents of the term in the ontology tree. Similar to get_parents but returns term names and term_ids instead of the term dbbact ids

    input:
    con,cur
    term : str
        The term ('feces' or 'efo:00001' etc.) for which to look for parents
    force_unique: bool, optional
        True to fail if more than one id matches the term
        False (default) to return parents for all the terms

    output:
    err : str
        Error message or empty string if ok
    terms : list of str
        the parent terms of the term (i.e. 'feces').
    term_ids: list of str
        the parent term_ids (i.e. 'gaz:000001'). same order as parents
    """
    err, parent_dbbact_ids = get_parents(con, cur, term, force_unique=force_unique)
    if err:
        return err, [], []
    err, terms, term_ids = get_names_from_ids(con, cur, parent_dbbact_ids)
    if err:
        return err, [], []
    return '', terms, term_ids


def get_family_graph(con, cur, terms, relation='both', force_unique=False, max_children_num=50):
    """
    get a cytoscape graph json of the parents and/or children of a term

    Parameters
    ----------
    con,cur
    term : list of str
        The terms ('feces' or 'efo:00001' etc.) for which to look for parents
    force_unique: bool, optional
        True to fail if more than one id matches the term
        False (default) to return parents for all the terms
    relation: str, optional
        'child' to get children, 'parent' to get parents, 'both' to get both
    max_children_num: int or None, optional
        if None, return all children
        if int, return maximum of max_children_num child results

    Returns
    -------
    err : str
        Error message or empty string if ok
    family : list of str
        json (cytospace graph) of the term parents and/or children
    """
    import networkx as nx

    # get the term ontologyids
    term_ids = []
    for cterm in terms:
        err, cterm_ids = get_term_ids(con, cur, cterm, allow_ontology_id=True)
        if err:
            msg = 'error getting term_ids for term %s: %s' % (cterm, err)
            debug(4, msg)
            return msg, []
        if len(cterm_ids) > 1:
            debug(1, '*** more than one id (%d) found for term %s. Please supply ontology id instead (i.e. "envo:00001")' % (len(cterm_ids), cterm))
            if force_unique:
                msg = 'more than one id (%d) found for term %s. Please supply ontology id instead (i.e. "envo:00001")' % (len(cterm_ids), cterm)
                debug(4, msg)
                return msg, []
        term_ids.extend(cterm_ids)

    tg = nx.DiGraph()
    if relation == 'both' or relation == 'parent':
        debug(3, 'Getting parents')
        plist = term_ids.copy()
        parents_ids = set(plist)
        processed_set = set()
        while len(plist) > 0:
            cid = plist.pop(0)
            if cid in processed_set:
                continue
            err, cparentids = GetTreeParentsById(con, cur, cid)
            if err:
                continue
            for ccparentid in cparentids:
                tg.add_edge(ccparentid, cid)
            plist.extend(cparentids)
            processed_set.add(cid)

    if relation == 'both' or relation == 'child':
        debug(3, 'Getting children')
        plist = term_ids.copy()
        parents_ids = set(plist)
        processed_set = processed_set.difference(term_ids)
        while len(plist) > 0:
            cid = plist.pop(0)
            if cid in processed_set:
                continue
            processed_set.add(cid)
            if max_children_num is not None:
                if len(processed_set) > max_children_num:
                    debug(3, 'max children num (%d) reached for terms: %s' % (max_children_num, terms))
                    break
            err, cparentids = GetTreeChildrenById(con, cur, cid)
            if err:
                continue
            for ccparentid in cparentids:
                tg.add_edge(cid, ccparentid)
            plist.extend(cparentids)

    # now add node names
    processed = list(processed_set)
    err, terms, term_ids = get_names_from_ids(con, cur, processed)
    for idx, cid in enumerate(processed):
        tg.nodes[cid]['name'] = terms[idx]
    debug(2, 'found %d parents' % len(parents_ids))
    return '', nx.node_link_data(tg)


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
    dbannotations._prepare_queries(con, cur)
    terms = tolist(terms)
    annotation_ids = None
    if len(terms) == 0:
        return 'No terms in query', []
    for cterm in terms:
        cterm = cterm.lower()
        # do we need to look for the term also as a parent of annotation terms?
        if get_children:
            cur.execute('SELECT idannotation FROM AnnotationParentsTable WHERE ontology=%s', [cterm])
            # if term not found in parents table, check if it is an id (i.e. envo:00000043 for wetland)
            if cur.rowcount == 0:
                err, ctermids = get_term_ids(con, cur, cterm)
                if err:
                    debug(2, err)
                    return err
                if len(ctermids) > 0:
                    # found it so it is an id. get also all the children
                    terms_from_id = set()
                    for cctermid in ctermids:
                        cur.execute('SELECT description FROM OntologyTable WHERE id=%s LIMIT 1', [cctermid])
                        if cur.rowcount == 0:
                            debug(6, 'description not found for termid %d' % cctermid)
                            continue
                        ccterm = cur.fetchone()[0]
                        terms_from_id.add(ccterm)
                    cur.execute('SELECT idannotation FROM AnnotationParentsTable WHERE ontology IN %s', [tuple(list(terms_from_id))])
                else:
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
            err, ctermids = get_term_ids(con, cur, cterm)
            if err:
                debug(2, err)
                return err
            if len(ctermids) == 0:
                if use_synonyms:
                    err, ctermid = GetSynonymTermId(con, cur, cterm)
                    ctermids = [ctermid]
                    if err:
                        msg = 'ontology term not found for %s' % cterm
                        debug(3, msg)
                        return msg, []
                    debug(2, 'converted synonym %s to termid %s' % (cterm, ctermids))
            cur.execute('SELECT idannotation FROM AnnotationListTable WHERE idontology IN %s', [tuple(ctermids)])

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


def get_term_onto_id_annotations(con, cur, terms, get_children=True):
    '''Get annotations for onotology ids (i.e. GAZ:00002476'''
    pass


def get_term_counts(con, cur, terms, term_types=('single'), ignore_lower=False):
    '''Get the number of annotations and experiments containing each term in terms.
    NOTE: terms can be also term pairs (term1+term2)

    Parameters
    ----------
    terms: list of str
        list of terms to look for. can be term pairs
    term_type:
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
        if term_types == ('single'):
            if cterm[0] == '-':
                cur.execute('SELECT exp_count, annotation_neg_count from OntologyTable WHERE description=%s LIMIT 1', [cterm[1:]])
            else:
                cur.execute('SELECT exp_count, annotationCount from OntologyTable WHERE description=%s LIMIT 1', [cterm])
        else:
            cur.execute('SELECT TotalExperiments, TotalAnnotations from TermInfoTable WHERE term=%s LIMIT 1', [cterm])
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
        for cdet in cannotation['details']:
            ctype = cdet[0]
            cterm = cdet[1]
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
    debug(4, 'GetListOfOntologies')

    if min_term_id is None:
        min_term_id = 0

    if ontologyid is None:
        cur.execute('SELECT id, description, term_id from ontologyTable WHERE id>%s', [min_term_id])
    else:
        cur.execute('SELECT ontologytreestructuretable.ontologyid, ontologytable.description, ontologytable.term_id FROM ontologytreestructuretable INNER JOIN ontologytable ON ontologytable.id=ontologytreestructuretable.ontologyid WHERE OntologyNameID=%s', [ontologyid])

    debug(3, 'found %d terms' % cur.rowcount)

    res = cur.fetchall()
    all_ontologies = {}
    all_ontology_ids = {}
    for cres in res:
        if cres[0] > min_term_id:
            all_ontologies[cres[1]] = cres[0]
            contologyid = cres[2]
            if contologyid == '':
                contologyid = 'dbbact:%d' % cres[0]
            all_ontology_ids[cres[0]] = contologyid
    return all_ontologies, all_ontology_ids


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


def get_parents_trees(con, cur, term, max_trees=20):
    '''Get trees for all parents of a given term
    Get at most max_trees (before subset removal)
    trees are filtered for subsets, so a tree which is a subset of another tree is not returned
    NOTE: this is not complete (not all trees are returned). use at your own risk

    Parameters
    ----------
    con, cur
    term: str
        the ontology term to look for parents
    max_trees: int, optional
        the maximal number of trees to look at (otherwise can take a very long time)

    Returns
    -------
    err, parent_trees
    err: str
        '' if ok, otherwise the error encountered
    parent_trees: list of [list of [str]]
        list of list of parents. each list of parents is ordered from the query term to the tree parent
    '''
    err, termids = GetIDs(con, cur, [term])
    if err:
        return err, []
    termids = termids[0]
    all_trees = []
    open_trees = [[x] for x in termids]
    total_trees = 0
    while len(open_trees) > 0:
        total_trees += 1
        if total_trees > max_trees:
            break
        res_tree = open_trees.pop()
        res_tree_set = set(res_tree)
        while True:
            ctermid = res_tree[-1]
            err, ids = GetTreeParentsById(con, cur, ctermid)
            # if reached top of tree
            if err != '':
                break
            # check for loops our tree
            ids = [x for x in ids if x not in res_tree_set]
            if len(ids) == 0:
                break
            # if we have more than 1 parent, process one and store the others
            if len(ids) > 1:
                for newterm in ids[1:]:
                    ttree = res_tree.copy()
                    ttree.append(newterm)
                    open_trees.append(ttree)
            ctermid = ids[0]
            res_tree.append(ctermid)
            res_tree_set.add(ctermid)
        # finished building this tree, add it to the list of all trees
        all_trees.append(res_tree)

    # remove all subsets
    all_ids = set()
    res_sets = [set(x) for x in all_trees]
    ok_trees = []
    for res1 in all_trees:
        set1 = set(res1)
        if set1.issubset(all_ids):
            continue
        all_ids = all_ids.union(set1)
        is_ok = True
        for set2 in res_sets:
            if set1 == set2:
                continue
            if set1.issubset(set2):
                is_ok = False
        if is_ok:
            ok_trees.append(res1)

    # convert ids to names
    all_tree_names = []
    for ctree in ok_trees:
        err, tree_names, alltaxids = get_names_from_ids(con, cur, ctree)
        if err == '':
            all_tree_names.append(tree_names)
    return '', all_tree_names
